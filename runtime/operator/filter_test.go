package operator

import (
	"testing"

	"github.com/wandouz/wstream/types"
)

type testFilter struct{}

func (t *testFilter) Filter(record types.Record) bool {
	return true
}

type testEmitter struct {
	length int
}

func (t *testEmitter) Emit(i types.Item)                {}
func (t *testEmitter) EmitTo(s int, i types.Item) error { return nil }
func (t *testEmitter) Dispose()                         {}
func (t *testEmitter) Length() int                      { return t.length }

func TestFilter(t *testing.T) {
	f := NewFilter(&testFilter{})
	emitter := &testEmitter{}
	f.New()
	f.handleRecord(nil, emitter)
	f.handleWatermark(nil, emitter)
}
