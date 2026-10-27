package jsplugin

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"time"

	"github.com/Calcium-Ion/moejs"
)

// AudioDurationProbe reads public audio without channel credentials. Its result
// is a reservation estimate; the provider's completion usage remains authoritative.
type AudioDurationProbe func(context.Context, string) (float64, error)

type audioDurationContextKey struct{}

func WithAudioDurationProbe(ctx context.Context, probe AudioDurationProbe) context.Context {
	return context.WithValue(ctx, audioDurationContextKey{}, probe)
}

func (c *runtimeCall) audioDuration(r *moejs.Realm, argument moejs.Value) (moejs.Value, error) {
	probe, _ := c.ctx.Value(audioDurationContextKey{}).(AudioDurationProbe)
	if probe == nil {
		return moejs.Undefined(), fmt.Errorf("utils.audioDuration is not available in %s", c.hook)
	}
	if !argument.IsString() {
		return moejs.Undefined(), r.TypeError("audio URL must be a string")
	}
	rawURL := argument.AsString().GoString()
	parsed, err := url.Parse(rawURL)
	if err != nil || len(rawURL) > maxFetchURLBytes || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return moejs.Undefined(), r.TypeError("audio URL must be an absolute HTTP(S) URL without credentials or fragment")
	}
	if c.audioProbes != 0 {
		return moejs.Undefined(), fmt.Errorf("utils.audioDuration allows one audio file per hook call")
	}
	c.audioProbes++
	if !c.watchdog.pause() {
		return moejs.Undefined(), r.CheckInterrupt()
	}
	c.yieldSlot()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	duration, probeErr := probe(ctx, rawURL)
	cancel()
	if !c.reclaimSlot() {
		c.runtime.Interrupt(fmt.Errorf("audio probe could not resume: %w", ErrCallAdmissionTimeout))
		return moejs.Undefined(), r.CheckInterrupt()
	}
	c.watchdog.resume()
	if err := r.CheckInterrupt(); err != nil {
		return moejs.Undefined(), err
	}
	if probeErr != nil {
		// Download/parser errors can contain signed URLs. Never expose them.
		return moejs.Undefined(), fmt.Errorf("audio duration probe failed; check the URL, format, size and duration limits")
	}
	if math.IsNaN(duration) || math.IsInf(duration, 0) || duration <= 0 {
		return moejs.Undefined(), fmt.Errorf("audio duration must be finite and positive")
	}
	return r.FromGo(duration)
}
