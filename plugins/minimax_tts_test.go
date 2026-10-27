package plugins_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/jsplugin"
	builtinplugins "github.com/QuantumNous/new-api/plugins"
	taskplugin "github.com/QuantumNous/new-api/relay/channel/task/jsplugin"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func miniMaxSpeechPlugin(t *testing.T) *jsplugin.LoadedPlugin {
	t.Helper()
	source, err := builtinplugins.Source("minimax-tts")
	require.NoError(t, err)
	plugin, err := jsplugin.CompilePlugin(source, jsplugin.Options{})
	require.NoError(t, err)
	return plugin
}

func TestMiniMaxSpeechCharacterEstimates(t *testing.T) {
	plugin := miniMaxSpeechPlugin(t)
	for _, tc := range []struct {
		text string
		want int
	}{
		{"Hello!", 6}, {"你好", 4}, {"A你好， B!", 9}, {"𠀀", 2}, {"😀", 1}, {"a\r\nb", 4}, {"é", 2},
		{strings.Repeat("中", 9999), 19998},
	} {
		t.Run(tc.text[:min(len(tc.text), 20)], func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/audio/speech", nil)
			c.Set("task_request", map[string]any{"input": tc.text, "voice": "female-shaonv", "speed": 1})
			info := &relaycommon.RelayInfo{OriginModelName: "speech-2.8-turbo", TaskRelayInfo: &relaycommon.TaskRelayInfo{}, ChannelMeta: &relaycommon.ChannelMeta{}}
			adaptor := taskplugin.New(plugin)
			adaptor.Init(info)
			facts, err := adaptor.ExtractUsageFactsValidated(c, info)
			require.NoError(t, err)
			assert.Equal(t, float64(tc.want), facts["characters"])
			ratios, err := adaptor.EstimateBillingValidated(c, info)
			require.NoError(t, err)
			assert.Empty(t, ratios)
		})
	}
}

func TestMiniMaxSpeechRejectsUnsupportedRequests(t *testing.T) {
	plugin := miniMaxSpeechPlugin(t)
	for _, tc := range []struct {
		name, field string
		value       any
	}{
		{"empty", "input", ""}, {"limit", "input", strings.Repeat("a", 10000)},
		{"input type", "input", 123}, {"voice", "voice", ""}, {"speed zero", "speed", 0}, {"speed high", "speed", 2.1}, {"speed string", "speed", "1"},
		{"sse", "stream_format", "sse"}, {"stream", "stream", true}, {"background", "background", true}, {"wav", "response_format", "wav"},
		{"metadata text", "metadata", map[string]any{"text": "free override"}}, {"metadata model", "metadata", map[string]any{"model": "other"}},
		{"spoofed usage", "characters", 0}, {"instructions", "instructions", "unsupported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{"input": "hello", "voice": "female-shaonv"}
			body[tc.field] = tc.value
			_, err := plugin.Engine.CallPath(t.Context(), "protocols", []string{"openai_speech", "decodeRequest"}, map[string]any{"model": "speech-2.8-turbo", "body": map[string]any{"kind": "json", "value": body}})
			require.Error(t, err)
		})
	}
}

func TestMiniMaxSpeechMappedRequestAndCompletedUsage(t *testing.T) {
	plugin := miniMaxSpeechPlugin(t)
	driver := map[string]any{"model": "speech-alias", "upstreamModel": "speech-2.8-turbo", "publicTaskId": "task_speech", "baseUrl": "https://api.minimax.cn/", "apiKey": "test-key", "requestBody": map[string]any{"input": "你好!", "voice": "female-shaonv", "speed": 0.5}}
	value, err := plugin.Engine.Call(t.Context(), "buildSubmitRequest", driver)
	require.NoError(t, err)
	request := decodePluginValue(t, value)
	assert.Equal(t, "https://api.minimax.cn/v1/t2a_v2", request["url"])
	body := request["body"].(map[string]any)
	assert.Equal(t, "speech-2.8-turbo", body["model"])
	assert.Equal(t, "你好!", body["text"])
	assert.Equal(t, false, body["stream"])
	assert.Equal(t, "url", body["output_format"])
	assert.Equal(t, float64(0.5), body["voice_setting"].(map[string]any)["speed"])
	assert.Equal(t, float64(0), body["voice_setting"].(map[string]any)["pitch"])
	assert.Equal(t, "Bearer test-key", request["headers"].(map[string]any)["Authorization"])
	for _, tc := range []struct {
		name  string
		usage any
		want  any
	}{
		{"lower", 3, float64(3)}, {"higher", 9, float64(9)}, {"zero", 0, float64(0)},
		{"missing", nil, nil}, {"negative", -1, nil}, {"fraction", 1.5, nil}, {"overflow", 19999, nil}, {"string", "5", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := map[string]any{"base_resp": map[string]any{"status_code": 0}, "data": map[string]any{"status": 2, "audio": "https://cdn.example/speech.mp3"}, "extra_info": map[string]any{"usage_characters": tc.usage, "audio_format": "mp3"}}
			value, err := plugin.Engine.Call(t.Context(), "parseSubmitResponse", driver, map[string]any{"body": response})
			require.NoError(t, err)
			parsed := decodePluginValue(t, value)
			assert.Equal(t, "task_speech", parsed["taskId"])
			data := parsed["taskData"].(map[string]any)
			assert.NotContains(t, data, "text")
			assert.Equal(t, tc.want, data["usage_characters"])
			facts, err := plugin.Engine.Call(t.Context(), "extractUsageOnComplete", nil, nil, data)
			require.NoError(t, err)
			if tc.want == nil {
				assert.Nil(t, facts)
			} else {
				assert.Equal(t, tc.want, decodePluginValue(t, facts)["characters"])
			}
			artifact, err := plugin.Engine.Call(t.Context(), "buildContentRequest", map[string]any{"artifactKey": "audio", "data": data})
			require.NoError(t, err)
			content := decodePluginValue(t, artifact)
			assert.Equal(t, true, content["credentialless"])
			assert.NotContains(t, content, "headers")
		})
	}
}

func TestMiniMaxSpeechMalformedSuccessNeverRetries(t *testing.T) {
	plugin := miniMaxSpeechPlugin(t)
	for _, body := range []string{`{broken`, `{"base_resp":{"status_code":1000}}`, `{"base_resp":{"status_code":0},"data":null}`} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/audio/speech", nil)
		c.Set(jsplugin.ContextKeyPinnedEndpoint, jsplugin.PinnedEndpoint{Protocol: jsplugin.ProtocolOpenAISpeech, Plugin: plugin})
		info := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{}, TaskRelayInfo: &relaycommon.TaskRelayInfo{}}
		adaptor := taskplugin.New(plugin)
		adaptor.Init(info)
		response := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}
		parsed, taskErr := adaptor.ParseResponse(c, response, info)
		require.Nil(t, parsed)
		require.NotNil(t, taskErr)
		assert.True(t, taskErr.NoRetry)
	}
	// The compact result remains a normal audio task, never an image result.
	artifacts, err := taskplugin.New(plugin).ListArtifacts(&model.Task{Status: model.TaskStatusSuccess, Data: []byte(`{"audio_url":"https://cdn.example/speech.mp3"}`)})
	require.NoError(t, err)
	require.Len(t, artifacts, 1)
	assert.Equal(t, "audio", artifacts[0].Type)
	assert.Equal(t, "audio/mpeg", artifacts[0].MimeType)
}
