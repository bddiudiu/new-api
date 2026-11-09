package jsplugin

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/service"
)

// probeAudioDuration is deliberately credentialless and uses the public-file
// SSRF transport, never the privileged channel API client. No audio is persisted.
func (a *TaskAdaptor) probeAudioDuration(ctx context.Context, rawURL string) (float64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if rawURL == a.audioProbeURL && a.audioSeconds > 0 {
		return a.audioSeconds, nil
	}
	if err := service.ValidateSSRFProtectedFetchURL(rawURL); err != nil {
		return 0, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, err
	}
	response, err := service.GetSSRFProtectedHTTPClient().Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("audio download returned HTTP %d", response.StatusCode)
	}
	// Bound memory even if the deployment's generic file limit is larger.
	limit := int64(min(max(constant.MaxFileDownloadMB, 1), 64)) << 20
	if response.ContentLength > limit {
		return 0, fmt.Errorf("audio file exceeds the download limit")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil || int64(len(data)) > limit {
		return 0, fmt.Errorf("audio download failed or exceeded the download limit")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return 0, fmt.Errorf("invalid audio URL")
	}
	ext := strings.ToLower(path.Ext(parsed.Path))
	mediaType, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
	switch mediaType {
	case "audio/mpeg", "audio/mp3":
		ext = ".mp3"
	case "audio/wav", "audio/x-wav", "audio/wave":
		ext = ".wav"
	case "audio/ogg", "application/ogg":
		ext = ".ogg"
	case "audio/flac", "audio/x-flac":
		ext = ".flac"
	case "audio/mp4", "video/mp4":
		ext = ".m4a"
	}
	// Keep parsing cancellation effective even for parsers that only read/seek.
	duration, err := common.GetAudioDuration(ctx, &audioProbeReader{ctx: ctx, Reader: bytes.NewReader(data)}, ext)
	if err != nil || math.IsNaN(duration) || math.IsInf(duration, 0) || duration <= 0 || duration > relaycommon.MaxTaskDurationSeconds {
		return 0, fmt.Errorf("audio must have a valid duration of at most %d seconds", relaycommon.MaxTaskDurationSeconds)
	}
	a.audioProbeURL, a.audioSeconds = rawURL, duration
	return duration, nil
}

type audioProbeReader struct {
	ctx context.Context
	*bytes.Reader
}

func (r *audioProbeReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

func (r *audioProbeReader) Seek(offset int64, whence int) (int64, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Seek(offset, whence)
}
