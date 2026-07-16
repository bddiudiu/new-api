package openai

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	relaycommon "github.com/QuantumNous/new-api/relay/common"

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
