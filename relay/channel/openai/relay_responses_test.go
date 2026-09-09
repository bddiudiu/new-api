package openai

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOaiResponsesHandlerUsesCurrentToolUsageSnapshot(t *testing.T) {
	oldMode := gin.Mode()
	gin.SetMode(gin.TestMode)
	t.Cleanup(func() { gin.SetMode(oldMode) })

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	info := &relaycommon.RelayInfo{
		ResponsesUsageInfo: &relaycommon.ResponsesUsageInfo{
			BuiltInTools: map[string]*relaycommon.BuildInToolInfo{
				"web_search": {ToolName: "web_search"},
			},
		},
	}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body: io.NopCloser(strings.NewReader(`{
			"output":[{"type":"web_search_call"},{"type":"web_search_call"}],
			"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2,"tool_usage":{"web_search":1}}
		}`)),
		Header: http.Header{"Content-Type": []string{"application/json"}},
	}

	usage, apiErr := OaiResponsesHandler(ctx, info, resp)

	require.Nil(t, apiErr)
	require.Equal(t, 2, usage.TotalTokens)
	require.Equal(t, 1, info.ResponsesUsageInfo.BuiltInTools["web_search"].CallCount)
}

func TestResponsesStreamFinalToolSnapshotReplacesProvisionalCalls(t *testing.T) {
	for _, terminal := range []string{"response.completed", "response.done"} {
		for _, snapshot := range []struct {
			name  string
			usage string
			want  int
		}{
			{"absent", "", 2},
			{"empty", `,"tool_usage":{}`, 0},
			{"partial", `,"tool_usage":{"file_search":1}`, 0},
			{"reported", `,"tool_usage":{"web_search_preview":1,"image_generation":20}`, 1},
			{"alias", `,"tool_usage":{"web_search":1}`, 1},
		} {
			t.Run(terminal+"/"+snapshot.name, func(t *testing.T) {
				info := runResponsesImageBillingStream(t,
					`{"type":"response.output_item.done","item":{"type":"web_search_call"}}`,
					`{"type":"response.output_item.done","item":{"type":"web_search_call"}}`,
					fmt.Sprintf(`{"type":%q,"response":{"status":"completed","usage":{"input_tokens":1,"output_tokens":1%s}}}`, terminal, snapshot.usage),
				)
				require.Equal(t, snapshot.want, info.ResponsesUsageInfo.BuiltInTools[dto.BuildInToolWebSearchPreview].CallCount)
				require.Zero(t, info.ResponsesUsageInfo.BuiltInTools[dto.BuildInToolImageGeneration].CallCount, "snapshots cannot charge nonexistent image outputs")
			})
		}
	}
}

func TestOaiStreamEmitsSeparateChoiceToolMetadataBeforeDone(t *testing.T) {
	oldTimeout := constant.StreamingTimeout
	constant.StreamingTimeout = 30
	t.Cleanup(func() { constant.StreamingTimeout = oldTimeout })
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	info := &relaycommon.RelayInfo{
		OriginModelName: "gpt-4o", RelayFormat: types.RelayFormatOpenAI,
		RelayMode: relayconstant.RelayModeChatCompletions, DisablePing: true,
		ChannelMeta: &relaycommon.ChannelMeta{UpstreamModelName: "gpt-4o"},
	}
	chunks := []string{
		`{"id":"chat-test","model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"weather","arguments":"{\"city\":"}}]}}]}`,
		`{"id":"chat-test","model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"Taipei\"}"}}]}}]}`,
		`{"id":"chat-test","model":"gpt-4o","choices":[{"index":1,"delta":{"tool_calls":[{"index":0,"id":"call-2","type":"function","function":{"name":"search","arguments":"{\"q\":"}}]}}]}`,
		`{"id":"chat-test","model":"gpt-4o","choices":[{"index":1,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"Go\"}"}}]}}]}`,
		`{"id":"chat-test","model":"gpt-4o","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`,
	}
	response := &http.Response{StatusCode: http.StatusOK,
		Header: http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:   io.NopCloser(strings.NewReader("data: " + strings.Join(chunks, "\n\ndata: ") + "\n\ndata: [DONE]\n\n")),
	}
	_, apiErr := OaiStreamHandler(ctx, info, response)
	require.Nil(t, apiErr)
	var metadata dto.ChatCompletionsStreamResponse
	count := 0
	for _, line := range strings.Split(recorder.Body.String(), "\n") {
		if !strings.HasPrefix(line, "data: ") || !strings.Contains(line, `"meta.info"`) {
			continue
		}
		require.NoError(t, common.UnmarshalJsonStr(strings.TrimPrefix(line, "data: "), &metadata))
		count++
	}
	require.Equal(t, 1, count)
	require.NotNil(t, metadata.MetaInfo)
	require.Len(t, metadata.MetaInfo.ToolCalls, 2)
	require.Equal(t, "call-1", metadata.MetaInfo.ToolCalls[0].ID)
	require.Equal(t, "weather", metadata.MetaInfo.ToolCalls[0].Function.Name)
	require.JSONEq(t, `{"city":"Taipei"}`, metadata.MetaInfo.ToolCalls[0].Function.Arguments)
	require.Equal(t, 0, *metadata.MetaInfo.ToolCalls[0].ChoiceIndex)
	require.Equal(t, "call-2", metadata.MetaInfo.ToolCalls[1].ID)
	require.Equal(t, "search", metadata.MetaInfo.ToolCalls[1].Function.Name)
	require.JSONEq(t, `{"q":"Go"}`, metadata.MetaInfo.ToolCalls[1].Function.Arguments)
	require.Equal(t, 1, *metadata.MetaInfo.ToolCalls[1].ChoiceIndex)
	require.True(t, strings.Index(recorder.Body.String(), `"meta.info"`) < strings.Index(recorder.Body.String(), "[DONE]"))
}
