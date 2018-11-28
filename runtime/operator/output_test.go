package operator

import (
	"testing"

	"github.com/wandouz/wstream/types"
)

type testOutput struct{}

func (t *testOutput) Output(r types.Record) {
}

func TestOutput(t *testing.T) {
	f := NewOutput(&testOutput{})
	emitter := &testEmitter{}
	f.New()
	f.handleRecord(nil, emitter)
	f.handleWatermark(nil, emitter)
}
