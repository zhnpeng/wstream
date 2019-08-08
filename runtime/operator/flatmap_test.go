package operator

import (
	"testing"

	"github.com/zhnpeng/wstream/functions"
	"github.com/zhnpeng/wstream/types"
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
