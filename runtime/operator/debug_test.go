package operator

import (
	"testing"

	"github.com/zhnpeng/wstream/types"
)

type testDebug struct{}

func (t *testDebug) Debug(r types.Record) {}

func TestDebug_New(t *testing.T) {
	f := NewDebug(&testDebug{})
	emitter := &testEmitter{}
	f.New()
	f.handleRecord(nil, emitter)
	f.handleWatermark(nil, emitter)
}
