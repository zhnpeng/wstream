package operator

import (
	"testing"

	"github.com/wandouz/wstream/types"
)

type testMap struct{}

func (t *testMap) Map(r types.Record) types.Record {
	return r
}
func TestMap(t *testing.T) {
	f := NewMap(&testMap{})
	emitter := &testEmitter{}
	f.New()
	f.handleRecord(nil, emitter)
	f.handleWatermark(nil, emitter)
}
