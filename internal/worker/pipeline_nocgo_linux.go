//go:build linux && !cgo

package worker

// The media pipeline needs GStreamer through cgo. A binary built without cgo keeps the worker
// role's command line and publisher but cannot produce media: Run fails with
// ErrPipelineUnsupported before touching the supervisor.

import (
	"errors"
	"time"
)

// ErrPipelineUnsupported reports a worker binary built without cgo.
var ErrPipelineUnsupported = errors.New("worker: media pipeline unavailable: this binary was built without cgo; build the worker on Linux with CGO_ENABLED=1 and the GStreamer development packages")

type pipeline struct{}

func openPipeline() (*pipeline, error)                  { return nil, ErrPipelineUnsupported }
func (p *pipeline) start() error                        { return ErrPipelineUnsupported }
func (p *pipeline) runningTime() (time.Duration, error) { return 0, ErrPipelineUnsupported }
func (p *pipeline) phases() (int64, int64, bool, bool)  { return 0, 0, false, false }
func (p *pipeline) origins() origins                    { return origins{} }
func (p *pipeline) push(int, []byte, time.Duration, time.Duration) (bool, error) {
	return false, ErrPipelineUnsupported
}
func (p *pipeline) endOfStream() error                             { return ErrPipelineUnsupported }
func (p *pipeline) pull(int, time.Duration) (*pulled, bool, error) { return nil, true, nil }
func (p *pipeline) poll(time.Duration) (bool, error)               { return true, nil }
func (p *pipeline) stop()                                          {}
func (p *pipeline) close()                                         {}
