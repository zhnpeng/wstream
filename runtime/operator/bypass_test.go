package operator

import (
	"testing"
)

func TestByPass_New(t *testing.T) {
	f := NewByPass()
	emitter := &testEmitter{}
	f.New()
	f.handleRecord(nil, emitter)
	f.handleWatermark(nil, emitter)
}
