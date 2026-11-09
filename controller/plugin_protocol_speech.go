package controller

import (
	"context"
	"net/http"
	"time"

	"github.com/QuantumNous/new-api/model"
	pluginruntime "github.com/QuantumNous/new-api/pkg/jsplugin"
	relaychannel "github.com/QuantumNous/new-api/relay/channel"
	taskjsplugin "github.com/QuantumNous/new-api/relay/channel/task/jsplugin"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/gin-gonic/gin"
)

// Speech renderers select an audio artifact; the host owns the binary response.
// The task has already settled. Download failures leave its artifact retrievable
// through the ordinary authenticated task content endpoint without resubmission.
func serveTaskPluginSpeechContent(c *gin.Context, pinned pluginruntime.PinnedEndpoint, task *model.Task, response map[string]any, info *relaycommon.RelayInfo) {
	key, ok := response["artifactKey"].(string)
	if !ok || len(response) != 1 || !taskArtifactKeyPattern.MatchString(key) {
		respondPluginProtocolError(c, http.StatusBadGateway, "task_protocol_error", "Speech renderer must select an audio artifact")
		return
	}
	adaptor := taskjsplugin.New(pinned.Plugin)
	adaptor.Init(info)
	artifacts, err := adaptor.ListArtifacts(task)
	if err != nil {
		writeTaskArtifactProjectionError(c, err)
		return
	}
	found := false
	for _, artifact := range artifacts {
		if artifact.Key == key && artifact.Type == "audio" && artifact.MimeType == "audio/mpeg" {
			found = true
			break
		}
	}
	if !found {
		respondPluginProtocolError(c, http.StatusBadGateway, "artifact_invalid_content", "Speech artifact must contain MP3 audio")
		return
	}
	request := c.Request
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Minute)
	defer cancel()
	c.Request = request.Clone(ctx)
	defer func() { c.Request = request }()
	descriptor, err := adaptor.BuildContentRequest(ctx, task, key, relaychannel.TaskArtifactClientRequest{Method: http.MethodGet})
	if err != nil {
		respondPluginProtocolError(c, http.StatusBadGateway, "artifact_plugin_error", "Artifact content plugin failed")
		return
	}
	if err := proxyTaskMediaWithPolicy(c, task, descriptor, &taskMediaDeliveryPolicy{mimeType: "audio/mpeg", maxBytes: 64 << 20}); err != nil {
		writeTaskMediaProxyError(c, err)
	}
}
