package operator

import (
	"testing"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/types"
)

type testFlatMap struct{}

func (t *testFlatMap) FlatMap(r types.Record, out functions.Emitter) {
}

func TestFlatMap(t *testing.T) {
	f := NewFlatMap(&testFlatMap{})
	emitter := &testEmitter{}
	f.New()
	f.handleRecord(nil, emitter)
	f.handleWatermark(nil, emitter)
}
