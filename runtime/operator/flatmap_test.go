package operator

import (
	"testing"

	"github.com/zhnpeng/wstream/funcintfs"
	"github.com/zhnpeng/wstream/types"
)

type testFlatMap struct{}

func (t *testFlatMap) FlatMap(r types.Record, out funcintfs.Emitter) {
}

func TestFlatMap(t *testing.T) {
	f := NewFlatMap(&testFlatMap{})
	emitter := &testEmitter{}
	f.New()
	f.handleRecord(nil, emitter)
	f.handleWatermark(nil, emitter)
}
