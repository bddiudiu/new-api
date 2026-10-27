package plugins_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/constant"
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

func TestMiniMaxSpeechSendsBearerChannelKey(t *testing.T) {
	plugin := miniMaxSpeechPlugin(t)
	info := &relaycommon.RelayInfo{
		ChannelMeta: &relaycommon.ChannelMeta{
			ChannelType:       constant.ChannelTypeTaskPlugin,
			ChannelBaseUrl:    "https://api.minimax.cn",
			ApiKey:            "minimax-channel-key",
			UpstreamModelName: "speech-2.8-turbo",
		},
		OriginModelName: "speech-2.8-turbo",
		TaskRelayInfo:   &relaycommon.TaskRelayInfo{PublicTaskID: "task_speech"},
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/audio/speech", nil)
	c.Request.Header.Set("Authorization", "Bearer client-gateway-token")
	c.Set("task_request", map[string]any{"input": "你好", "voice": "male-qn-qingse"})
	adaptor := taskplugin.New(plugin)
	adaptor.Init(info)
	// Exercise the host credential context: api_key authHeader is the raw key.
	body, err := adaptor.BuildRequestBody(c, info)
	require.NoError(t, err)
	url, err := adaptor.BuildRequestURL(info)
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, url, body)
	require.NoError(t, adaptor.BuildRequestHeader(c, request, info))
	assert.Equal(t, "Bearer minimax-channel-key", request.Header.Get("Authorization"))
	assert.Equal(t, "https", request.URL.Scheme)
	assert.Empty(t, request.URL.RawQuery)
	assert.Nil(t, request.URL.User)
	encoded, err := io.ReadAll(request.Body)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "minimax-channel-key")
	assert.NotContains(t, string(encoded), "client-gateway-token")
}

func TestMiniMaxSpeechMalformedSuccessNeverRetries(t *testing.T) {
	plugin := miniMaxSpeechPlugin(t)
	for _, tc := range []struct {
		name, body, message string
	}{
		{"malformed JSON", `{broken`, "invalid JSON response; check the channel Base URL"},
		{"unexpected envelope", `{"error":{"message":"private-upstream-detail"}}`, "response is missing base_resp"},
		{"invalid envelope", `{"base_resp":[]}`, "response is missing base_resp"},
		{"missing status", `{"base_resp":{}}`, "invalid base_resp.status_code"},
		{"string status", `{"base_resp":{"status_code":"0"}}`, "invalid base_resp.status_code"},
		{"fractional status", `{"base_resp":{"status_code":1004.5}}`, "invalid base_resp.status_code"},
		{"authentication", `{"base_resp":{"status_code":1004,"status_msg":"Bearer private-api-key https://cdn.example/private-audio?signature=private-signature\nprivate-upstream-detail"},"trace_id":"private-trace"}`, "status_code=1004): Authentication failed"},
		{"balance", `{"base_resp":{"status_code":1008}}`, "status_code=1008): Insufficient MiniMax account balance"},
		{"voice", `{"base_resp":{"status_code":2043}}`, "status_code=2043): The model does not support this voice_id"},
		{"unknown status", `{"base_resp":{"status_code":9876,"status_msg":"private-upstream-detail"}}`, "status_code=9876): See the MiniMax error code documentation"},
		{"missing audio", `{"base_resp":{"status_code":0},"data":null}`, "MiniMax returned no completed audio URL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/audio/speech", nil)
			c.Set(jsplugin.ContextKeyPinnedEndpoint, jsplugin.PinnedEndpoint{Protocol: jsplugin.ProtocolOpenAISpeech, Plugin: plugin})
			info := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{}, TaskRelayInfo: &relaycommon.TaskRelayInfo{}}
			adaptor := taskplugin.New(plugin)
			adaptor.Init(info)
			response := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(tc.body))}
			parsed, taskErr := adaptor.ParseResponse(c, response, info)
			require.Nil(t, parsed)
			require.NotNil(t, taskErr)
			assert.Equal(t, "plugin_submit_response_failed", taskErr.Code)
			assert.Equal(t, http.StatusBadGateway, taskErr.StatusCode)
			assert.Contains(t, taskErr.Message, tc.message)
			assert.NotContains(t, taskErr.Message, "private-")
			assert.NotContains(t, taskErr.Error.Error(), "private-")
			assert.True(t, taskErr.NoRetry)
		})
	}
	// The compact result remains a normal audio task, never an image result.
	artifacts, err := taskplugin.New(plugin).ListArtifacts(&model.Task{Status: model.TaskStatusSuccess, Data: []byte(`{"audio_url":"https://cdn.example/speech.mp3"}`)})
	require.NoError(t, err)
	require.Len(t, artifacts, 1)
	assert.Equal(t, "audio", artifacts[0].Type)
	assert.Equal(t, "audio/mpeg", artifacts[0].MimeType)
}
