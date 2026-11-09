package openai

import (
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObserveStreamChoicesDedupesFunctionCallNames(t *testing.T) {
	info := &relaycommon.RelayInfo{StreamStatus: relaycommon.NewStreamStatus()}
	seen := make(map[string]struct{})
	var names []string

	chunks := []string{
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c1","type":"function","function":{"name":"get_weather","arguments":""}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"q\":"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"x\"}"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"c2","type":"function","function":{"name":"get_time","arguments":""}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"function":{"arguments":"{}"}}]}}]}`,
	}
	for _, chunk := range chunks {
		observeStreamChoices(info, chunk, seen, &names)
	}

	require.Len(t, names, 2)
	assert.Equal(t, []string{"get_weather", "get_time"}, names)
	assert.Empty(t, info.StreamStatus.ResponseOutcome(), "tool call deltas carry no finish reason")

	observeStreamChoices(info, `{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`, seen, &names)
	assert.Equal(t, "completed", info.StreamStatus.ResponseOutcome())
	assert.False(t, info.PerformanceBusinessRejection)

	filtered := &relaycommon.RelayInfo{StreamStatus: relaycommon.NewStreamStatus()}
	observeStreamChoices(filtered, `{"choices":[{"index":0,"delta":{},"finish_reason":"content_filter"}]}`, map[string]struct{}{}, &names)
	assert.True(t, filtered.PerformanceBusinessRejection)
}

func TestOaiStreamToolMetadataPreservesCallsAndUsage(t *testing.T) {
	previousTimeout := constant.StreamingTimeout
	constant.StreamingTimeout = 30
	t.Cleanup(func() { constant.StreamingTimeout = previousTimeout })
	operation_setting.SetToolPriceForTest("metainfo_other", 1)
	t.Cleanup(func() { operation_setting.DeleteToolPriceForTest("metainfo_other") })
	for _, tc := range []struct {
		name                                                      string
		includeUsage, upstreamUsage, penultimate, force, thinking bool
	}{
		{name: "upstream usage", includeUsage: true, upstreamUsage: true},
		{name: "hidden usage", upstreamUsage: true},
		{name: "synthesized usage", includeUsage: true},
		{name: "penultimate usage", includeUsage: true, upstreamUsage: true, penultimate: true},
		{name: "force format", includeUsage: true, upstreamUsage: true, force: true},
		{name: "thinking conversion", includeUsage: true, upstreamUsage: true, thinking: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chunks := []string{
				`data: {"id":"chat_meta","model":"gpt-test","created":123,"choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"reason"}}]}`,
				`data: {"id":"chat_meta","model":"gpt-test","created":123,"choices":[{"index":0,"delta":{"content":"answer"}}]}`,
				`data: {"id":"chat_meta","model":"gpt-test","created":123,"choices":[{"index":1,"delta":{"tool_calls":[{"index":0,"id":"choice1","type":"function","function":{"name":"metainfo_other","arguments":"{\"c\":"}}]}},{"index":0,"delta":{"tool_calls":[{"index":1,"id":"call_time","type":"function","function":{"name":"time","arguments":"{"}},{"index":0,"id":"call_weather","type":"function","function":{"name":"get_","arguments":"{\"city\":"}}]}}]}`,
				`data: {"id":"chat_meta","model":"gpt-test","created":123,"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"weather","arguments":"\"Taipei\"}"}},{"index":1,"function":{"arguments":"}"}}]}},{"index":1,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}}]}}]}`,
				`data: {"id":"chat_meta","model":"gpt-test","created":123,"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
			}
			if tc.upstreamUsage {
				chunks = append(chunks, `data: {"id":"chat_meta","model":"gpt-test","created":123,"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":9,"total_tokens":14}}`)
			}
			if tc.penultimate {
				chunks = append(chunks, `data: {"id":"chat_meta","model":"gpt-test","created":123,"choices":[]}`)
			}
			chunks = append(chunks, "data: [DONE]", "")
			c, recorder, resp, info := newResponsesChatTestContext(t, strings.Join(chunks, "\n\n"), true)
			info.RelayMode = relayconstant.RelayModeChatCompletions
			info.ShouldIncludeUsage = tc.includeUsage
			info.ChannelSetting.ForceFormat = tc.force
			info.ChannelSetting.ThinkingToContent = tc.thinking
			info.ThinkingContentInfo.IsFirstThinkingContent = true
			usage, apiErr := OaiStreamHandler(c, info, resp)
			require.Nil(t, apiErr)
			require.NotNil(t, usage)
			if tc.upstreamUsage {
				assert.Equal(t, 5, usage.PromptTokens)
				assert.Equal(t, 9, usage.CompletionTokens)
			}
			var metas []dto.ChatCompletionsStreamResponse
			usageFrames := 0
			var content strings.Builder
			for line := range strings.SplitSeq(recorder.Body.String(), "\n") {
				data, ok := strings.CutPrefix(line, "data: ")
				if !ok || data == "[DONE]" {
					continue
				}
				var frame dto.ChatCompletionsStreamResponse
				require.NoError(t, common.UnmarshalJsonStr(data, &frame))
				if frame.Type == "meta.info" {
					metas = append(metas, frame)
				}
				if frame.Usage != nil {
					usageFrames++
				}
				for _, choice := range frame.Choices {
					content.WriteString(choice.Delta.GetContentString())
				}
			}
			require.Len(t, metas, 1)
			assert.Equal(t, "chat_meta", metas[0].Id)
			require.NotNil(t, metas[0].MetaInfo)
			calls := metas[0].MetaInfo.ToolCalls
			require.Len(t, calls, 3)
			for i, want := range []struct {
				id, name, arguments string
				index               int
			}{
				{"call_weather", "get_weather", `{"city":"Taipei"}`, 0},
				{"call_time", "time", `{}`, 1},
				{"choice1", "metainfo_other", `{"c":1}`, 0},
			} {
				assert.Equal(t, want.id, calls[i].ID)
				assert.Equal(t, want.name, calls[i].Function.Name)
				assert.JSONEq(t, want.arguments, calls[i].Function.Arguments)
				require.NotNil(t, calls[i].Index)
				assert.Equal(t, want.index, *calls[i].Index)
				assert.Equal(t, "function", calls[i].Type)
			}
			if tc.includeUsage {
				assert.Equal(t, 1, usageFrames)
			} else {
				assert.Zero(t, usageFrames)
			}
			assert.Equal(t, 1, strings.Count(recorder.Body.String(), "data: [DONE]"))
			requireOrderedSubstrings(t, recorder.Body.String(), `"type":"meta.info"`, "data: [DONE]")
			if !tc.upstreamUsage {
				requireOrderedSubstrings(t, recorder.Body.String(), `"type":"meta.info"`, `"usage":{`, "data: [DONE]")
			}
			if tc.thinking {
				assert.Equal(t, "<think>\nreason\n</think>\nanswer", content.String())
			}
			require.NotNil(t, info.ResponsesUsageInfo)
			require.Contains(t, info.ResponsesUsageInfo.BuiltInTools, "metainfo_other")
			assert.Equal(t, 1, info.ResponsesUsageInfo.BuiltInTools["metainfo_other"].CallCount, "metadata must not become another billable call")
		})
	}
}

func TestOaiStreamToolMetadataFormatAndEmptyCalls(t *testing.T) {
	previousTimeout := constant.StreamingTimeout
	constant.StreamingTimeout = 30
	t.Cleanup(func() { constant.StreamingTimeout = previousTimeout })
	for _, tc := range []struct {
		name   string
		format types.RelayFormat
		tools  bool
	}{
		{"no calls", types.RelayFormatOpenAI, false},
		{"missing index", types.RelayFormatOpenAI, true},
		{"Claude", types.RelayFormatClaude, true},
		{"Gemini", types.RelayFormatGemini, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delta := `{"role":"assistant","content":"hello"}`
			if tc.tools {
				delta = `{"role":"assistant","tool_calls":[{"id":"single","type":"function","function":{"name":"lookup","arguments":"{}"}}]}`
			}
			body := "data: {\"id\":\"chat_single\",\"model\":\"gpt-test\",\"choices\":[{\"index\":0,\"delta\":" + delta + "}]}\n\n" +
				`data: {"id":"chat_single","model":"gpt-test","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}` + "\n\ndata: [DONE]\n\n"
			c, recorder, resp, info := newResponsesChatTestContext(t, body, true)
			info.RelayMode = relayconstant.RelayModeChatCompletions
			info.RelayFormat = tc.format
			info.EnsureClaudeConvertInfo()
			usage, apiErr := OaiStreamHandler(c, info, resp)
			require.Nil(t, apiErr)
			assert.Equal(t, 2, usage.PromptTokens)
			assert.Equal(t, 3, usage.CompletionTokens)
			if tc.tools && tc.format == types.RelayFormatOpenAI {
				assert.Contains(t, recorder.Body.String(), `"type":"meta.info"`)
				for line := range strings.SplitSeq(recorder.Body.String(), "\n") {
					data, ok := strings.CutPrefix(line, "data: ")
					if !ok || data == "[DONE]" {
						continue
					}
					var event dto.ChatCompletionsStreamResponse
					require.NoError(t, common.UnmarshalJsonStr(data, &event))
					if event.MetaInfo != nil {
						require.Len(t, event.MetaInfo.ToolCalls, 1)
						call := event.MetaInfo.ToolCalls[0]
						require.NotNil(t, call.Index)
						assert.Zero(t, *call.Index)
						assert.Equal(t, "single", call.ID)
					}
				}
			} else {
				assert.NotContains(t, recorder.Body.String(), `"metainfo"`)
			}
		})
	}
}
