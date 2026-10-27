package plugins_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/billingexpr"
	"github.com/QuantumNous/new-api/pkg/jsplugin"
	builtinplugins "github.com/QuantumNous/new-api/plugins"
	"github.com/QuantumNous/new-api/relay/channel"
	taskplugin "github.com/QuantumNous/new-api/relay/channel/task/jsplugin"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/system_setting"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func speechPlugin(t *testing.T, key string) *jsplugin.LoadedPlugin {
	t.Helper()
	source, err := builtinplugins.Source(key)
	require.NoError(t, err)
	plugin, err := jsplugin.CompilePlugin(source, jsplugin.Options{})
	require.NoError(t, err)
	return plugin
}

func TestSpeechPluginsMeterAndValidateRequests(t *testing.T) {
	for _, tc := range []struct {
		key, asr, tts string
		characters    float64
		seconds       float64
	}{
		{"alibaba-speech", "fun-asr", "qwen-audio-3.0-tts-flash", 8, 2},
		{"volcengine-speech", "volc.seedasr.auc", "seed-tts-2.0", 6, 1.25},
	} {
		t.Run(tc.key, func(t *testing.T) {
			plugin := speechPlugin(t, tc.key)
			schema, _ := plugin.Meta.UsageForModels(tc.asr)
			assert.Equal(t, "second", schema["audio_seconds"].Unit)
			assert.NotContains(t, schema, "characters")
			ctx := jsplugin.WithAudioDurationProbe(t.Context(), func(_ context.Context, url string) (float64, error) {
				assert.Equal(t, "https://media.example/audio.wav", url)
				return 1.25, nil
			})
			driver := map[string]any{"action": "speech_to_text", "model": "asr-alias", "upstreamModel": tc.asr,
				"publicTaskId": "task_asr", "baseUrl": "https://vendor.example", "apiKey": "channel-key",
				"requestBody": map[string]any{"audio_url": "https://media.example/audio.wav"}}
			value, err := plugin.Engine.Call(ctx, "buildSubmitRequest", driver)
			require.NoError(t, err)
			descriptor := decodePluginValue(t, value)
			assert.Equal(t, "POST", descriptor["method"])
			assert.NotContains(t, descriptor, "model", "mapped billing alias must not be rewritten")
			value, err = plugin.Engine.Call(ctx, "extractUsage", driver)
			require.NoError(t, err)
			assert.Equal(t, tc.seconds, decodePluginValue(t, value)["audio_seconds"])
			body := descriptor["body"].(map[string]any)
			if tc.key == "alibaba-speech" {
				assert.Equal(t, tc.asr, body["model"])
				assert.Equal(t, []any{float64(0)}, body["parameters"].(map[string]any)["channel_id"])
			} else {
				assert.Equal(t, tc.asr, descriptor["headers"].(map[string]any)["X-Api-Resource-Id"])
				assert.Equal(t, "bigmodel", body["request"].(map[string]any)["model_name"])
			}

			driver["action"], driver["upstreamModel"] = "text_to_speech", tc.tts
			driver["requestBody"] = map[string]any{"input": "中A文123", "voice": "voice-test"}
			value, err = plugin.Engine.Call(t.Context(), "extractUsage", driver)
			require.NoError(t, err)
			assert.Equal(t, tc.characters, decodePluginValue(t, value)["characters"])
			value, err = plugin.Engine.Call(t.Context(), "buildSubmitRequest", driver)
			require.NoError(t, err)
			assert.NotContains(t, decodePluginValue(t, value), "model")
			for _, unsupported := range []map[string]any{
				{"model": tc.asr, "audio_url": "file:///private.wav"},
				{"model": tc.asr, "audio_url": "https://secret@host/audio.wav"},
				{"model": tc.asr, "audio_url": "https://media.example/audio.wav", "duration": 0},
				{"model": tc.asr, "audio_url": "https://media.example/audio.wav", "metadata": map[string]any{"model": "other"}},
			} {
				_, err := plugin.Engine.CallPath(t.Context(), "native", []string{"transcribe"}, map[string]any{"body": map[string]any{"kind": "json", "value": unsupported}})
				require.Error(t, err)
			}
			_, err = plugin.Engine.CallPath(t.Context(), "native", []string{"transcribe"}, map[string]any{"body": map[string]any{"kind": "multipart"}})
			require.ErrorContains(t, err, "file uploads are not supported")
			for _, extra := range []map[string]any{{"stream": true}, {"response_format": "wav"}, {"characters": 0}, {"input": strings.Repeat("x", 20001)}} {
				body := map[string]any{"model": tc.tts, "input": "hello", "voice": "voice"}
				for k, v := range extra {
					body[k] = v
				}
				_, err := plugin.Engine.CallPath(t.Context(), "protocols", []string{"openai_speech", "decodeRequest"}, map[string]any{"model": tc.tts, "body": map[string]any{"kind": "json", "value": body}})
				require.Error(t, err)
			}
		})
	}
}

func TestSpeechCompletionUsageAndFrozenPrices(t *testing.T) {
	for _, tc := range []struct {
		key, action, field string
		body               string
		want               float64
	}{
		{"alibaba-speech", "speech_to_text", "audio_seconds", `{"usage":{"duration":1.5}}`, 1.5},
		{"volcengine-speech", "speech_to_text", "audio_seconds", `{"audio_info":{"duration":1500}}`, 1.5},
		{"alibaba-speech", "text_to_speech", "characters", `{"usage_characters":3}`, 3},
		{"volcengine-speech", "text_to_speech", "characters", `{"data":{"req_text_length":3,"synthesize_text_length":1}}`, 3},
		{"alibaba-speech", "speech_to_text", "audio_seconds", `{"usage":{"duration":0}}`, 0},
		{"volcengine-speech", "text_to_speech", "characters", `{"data":{"req_text_length":0}}`, 0},
	} {
		t.Run(tc.key+tc.body, func(t *testing.T) {
			plugin := speechPlugin(t, tc.key)
			var body any
			require.NoError(t, common.UnmarshalJsonStr(tc.body, &body))
			value, err := plugin.Engine.Call(t.Context(), "extractUsageOnComplete", map[string]any{"action": tc.action}, nil, body)
			require.NoError(t, err)
			facts := decodePluginValue(t, value)
			assert.Equal(t, tc.want, facts[tc.field])
			expr := `tier("audio", u("` + tc.field + `") * 0.01)`
			snap := &billingexpr.BillingSnapshot{ExprString: expr, ExprHash: billingexpr.ExprHashString(expr), ExprVersion: 1, TaskUsageBilling: true, QuotaPerUnit: 100000, GroupRatio: 2, UsageFacts: map[string]any{tc.field: 10.0}}
			result, settled, err := service.EvaluateTaskCompletionUsage(snap, facts)
			require.NoError(t, err)
			assert.EqualValues(t, tc.want*2000, result.ActualQuotaAfterGroup)
			assert.Equal(t, tc.want, settled[tc.field])
		})
	}
	for _, key := range []string{"alibaba-speech", "volcengine-speech"} {
		plugin := speechPlugin(t, key)
		for _, n := range []any{nil, -1, "12", 3600001} {
			body := map[string]any{"usage": map[string]any{"duration": n}, "audio_info": map[string]any{"duration": n}}
			value, err := plugin.Engine.Call(t.Context(), "extractUsageOnComplete", map[string]any{"action": "speech_to_text"}, nil, body)
			require.NoError(t, err)
			assert.Nil(t, value, "%s %v", key, n)
		}
	}
}

func TestSpeechProviderTaskResponses(t *testing.T) {
	ali := speechPlugin(t, "alibaba-speech")
	for _, tc := range []struct{ body, status string }{
		{`{"output":{"task_status":"RUNNING"}}`, "IN_PROGRESS"},
		{`{"output":{"task_status":"SUCCEEDED","results":[{"subtask_status":"FAILED"}]}}`, "FAILURE"},
		{`{"output":{"task_status":"SUCCEEDED","results":[{"subtask_status":"SUCCEEDED","transcription_url":"https://result.example/transcript.json"}]}}`, "SUCCESS"},
		{`{"output":{"task_status":"NEW_STATUS"}}`, "UNKNOWN"},
	} {
		var body any
		require.NoError(t, common.UnmarshalJsonStr(tc.body, &body))
		value, err := ali.Engine.Call(t.Context(), "parseTaskResult", nil, body)
		require.NoError(t, err)
		assert.Equal(t, tc.status, decodePluginValue(t, value)["status"])
	}
	volc := speechPlugin(t, "volcengine-speech")
	ctx := map[string]any{"action": "speech_to_text", "publicTaskId": "public-id"}
	value, err := volc.Engine.Call(t.Context(), "parseSubmitResponse", ctx, map[string]any{"headers": map[string]any{"X-Api-Status-Code": []any{"20000000"}}, "body": map[string]any{"task_id": "vendor-task"}})
	require.NoError(t, err)
	assert.Equal(t, "vendor-task", decodePluginValue(t, value)["taskId"])
	for code, status := range map[string]string{"20000000": "SUCCESS", "20000001": "IN_PROGRESS", "20000002": "QUEUED", "20000003": "SUCCESS", "45000001": "FAILURE", "55000031": "UNKNOWN", "": "UNKNOWN"} {
		value, err := volc.Engine.Call(t.Context(), "parseTaskResult", ctx, map[string]any{"result": map[string]any{"text": "hello"}}, map[string]any{"headers": map[string]any{"X-Api-Status-Code": code}})
		require.NoError(t, err)
		assert.Equal(t, status, decodePluginValue(t, value)["status"], code)
	}
	// Polling stores compact audio metadata, while completion meters the original response.
	adaptor := taskplugin.New(volc)
	adaptor.Init(&relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{ChannelType: constant.ChannelTypeTaskPlugin, ChannelBaseUrl: "https://openspeech.bytedance.com", ApiKey: "key"}})
	task := &model.Task{TaskID: "public", Action: "text_to_speech", Properties: model.Properties{OriginModelName: "seed-tts-2.0", UpstreamModelName: "seed-tts-2.0"}, PrivateData: model.TaskPrivateData{UpstreamTaskID: "vendor"}}
	result, err := adaptor.ParseTaskResult(task, &http.Response{StatusCode: 200, Header: http.Header{}}, []byte(`{"code":20000000,"data":{"task_status":2,"audio_url":"https://cdn.example/a.mp3","req_text_length":6,"synthesize_text_length":5,"sentences":[{"text":"private spoken text"}]}}`))
	require.NoError(t, err)
	assert.Equal(t, "SUCCESS", result.Status)
	assert.NotContains(t, string(result.PluginTaskData), "private spoken text")
	assert.NotContains(t, string(result.PluginTaskData), "sentences")
	assert.Equal(t, float64(6), result.UsageFacts["characters"])
}

func TestSpeechAudioProbeDownloadsOnceAndRejectsPrivateURLs(t *testing.T) {
	service.InitHttpClient()
	setting := system_setting.GetFetchSetting()
	previous := *setting
	t.Cleanup(func() { *setting = previous })
	*setting = system_setting.FetchSetting{EnableSSRFProtection: true, AllowPrivateIp: true, AllowedPorts: []string{"1-65535"}}
	previousCountToken := constant.CountToken
	constant.CountToken = false
	t.Cleanup(func() { constant.CountToken = previousCountToken })
	var wav bytes.Buffer
	wav.WriteString("RIFF")
	require.NoError(t, binary.Write(&wav, binary.LittleEndian, uint32(36+16000)))
	wav.WriteString("WAVEfmt ")
	for _, field := range []any{uint32(16), uint16(1), uint16(1), uint32(8000), uint32(16000), uint16(2), uint16(16)} {
		require.NoError(t, binary.Write(&wav, binary.LittleEndian, field))
	}
	wav.WriteString("data")
	require.NoError(t, binary.Write(&wav, binary.LittleEndian, uint32(16000)))
	wav.Write(make([]byte, 16000))
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		assert.Empty(t, r.Header.Get("Authorization"))
		assert.Empty(t, r.Header.Get("X-Api-Key"))
		w.Header().Set("Content-Type", "audio/wav")
		_, _ = w.Write(wav.Bytes())
	}))
	defer server.Close()
	plugin := speechPlugin(t, "alibaba-speech")
	info := &relaycommon.RelayInfo{OriginModelName: "fun-asr", TaskRelayInfo: &relaycommon.TaskRelayInfo{Action: "speech_to_text", PublicTaskID: "task"}, ChannelMeta: &relaycommon.ChannelMeta{ChannelType: constant.ChannelTypeTaskPlugin, ChannelBaseUrl: "https://dashscope.aliyuncs.com", ApiKey: "secret", UpstreamModelName: "fun-asr"}}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/alibaba-speech/v1/audio/transcriptions", nil)
	c.Set("task_request", map[string]any{"audio_url": server.URL + "/audio.wav"})
	adaptor := taskplugin.New(plugin)
	adaptor.Init(info)
	_, err := adaptor.BuildRequestBody(c, info)
	require.NoError(t, err)
	facts, err := adaptor.ExtractUsageFactsValidated(c, info)
	require.NoError(t, err)
	assert.Equal(t, float64(1), facts["audio_seconds"])
	assert.Equal(t, 1, calls)
	setting.AllowPrivateIp = false
	adaptor = taskplugin.New(plugin)
	adaptor.Init(info)
	_, err = adaptor.BuildRequestBody(c, info)
	require.Error(t, err)
	assert.Equal(t, 1, calls, "rejected private URL must not be fetched")
	assert.NotContains(t, err.Error(), server.URL)
}

func TestSpeechAudioProbeContextAndErrorIsolation(t *testing.T) {
	engine, err := jsplugin.Compile(`export function run(url) { return utils.audioDuration(url); }
export function twice(url) { utils.audioDuration(url); return utils.audioDuration(url); }`, jsplugin.Options{})
	require.NoError(t, err)
	_, err = engine.Call(t.Context(), "run", "https://audio.example/a.wav")
	require.ErrorContains(t, err, "not available")
	calls := 0
	ctx := jsplugin.WithAudioDurationProbe(t.Context(), func(context.Context, string) (float64, error) { calls++; return 2.5, nil })
	_, err = engine.Call(ctx, "twice", "https://audio.example/a.wav")
	require.ErrorContains(t, err, "one audio file")
	assert.Equal(t, 1, calls)
	ctx = jsplugin.WithAudioDurationProbe(t.Context(), func(context.Context, string) (float64, error) {
		return 0, errors.New("https://private.example/a?token=secret")
	})
	_, err = engine.Call(ctx, "run", "https://audio.example/a.wav?signature=secret")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "secret")
	assert.NotContains(t, err.Error(), "private.example")
}

func TestSpeechArtifactRefreshDoesNotResubmitOrSettle(t *testing.T) {
	service.InitHttpClient()
	queries := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries++
		assert.Equal(t, "/api/v3/tts/query", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "selected-key", r.Header.Get("X-Api-Key"))
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.JSONEq(t, `{"task_id":"vendor-id"}`, string(body))
		data, err := common.Marshal(map[string]any{"code": 20000000, "data": map[string]any{"task_status": 2, "audio_url": "https://cdn.example/fresh.mp3", "url_expire_time": time.Now().Unix() + 3600}})
		require.NoError(t, err)
		_, _ = w.Write(data)
	}))
	defer server.Close()
	plugin := speechPlugin(t, "volcengine-speech")
	adaptor := taskplugin.New(plugin)
	adaptor.Init(&relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{ChannelType: constant.ChannelTypeTaskPlugin, ChannelBaseUrl: server.URL, ApiKey: "current-key"}})
	task := &model.Task{TaskID: "public-id", Action: "text_to_speech", Status: model.TaskStatusSuccess, Quota: 123,
		Data:        []byte(`{"data":{"audio_url":"https://cdn.example/expired.mp3","url_expire_time":1}}`),
		Properties:  model.Properties{OriginModelName: "seed-tts-2.0", UpstreamModelName: "seed-tts-2.0"},
		PrivateData: model.TaskPrivateData{Key: "selected-key", UpstreamTaskID: "vendor-id"}}
	prior := string(task.Data)
	descriptor, err := adaptor.BuildContentRequest(t.Context(), task, "audio", channel.TaskArtifactClientRequest{Method: http.MethodGet})
	require.NoError(t, err)
	assert.Equal(t, "https://cdn.example/fresh.mp3", descriptor.URL)
	assert.True(t, descriptor.Credentialless)
	assert.Empty(t, descriptor.Headers)
	assert.Equal(t, 1, queries)
	assert.Equal(t, 123, task.Quota)
	assert.EqualValues(t, model.TaskStatusSuccess, task.Status)
	assert.Equal(t, prior, string(task.Data))
}

func TestSpeechNativeAcceptedResponseFailuresNeverRetry(t *testing.T) {
	for _, key := range []string{"alibaba-speech", "volcengine-speech"} {
		for _, action := range []string{"speech_to_text", "text_to_speech"} {
			t.Run(key+action, func(t *testing.T) {
				plugin := speechPlugin(t, key)
				adaptor := taskplugin.New(plugin)
				info := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{}, TaskRelayInfo: &relaycommon.TaskRelayInfo{Action: action}}
				adaptor.Init(info)
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = httptest.NewRequest(http.MethodPost, "/"+key+"/v1/audio/speech", nil)
				response := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{broken`))}
				defer response.Body.Close()
				parsed, taskErr := adaptor.ParseResponse(c, response, info)
				require.Nil(t, parsed)
				require.NotNil(t, taskErr)
				assert.True(t, taskErr.NoRetry)
			})
		}
	}
}
