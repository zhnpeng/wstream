package stream

import (
	"testing"
	"time"

	"github.com/wandouz/wstream/types"
)

type reduceFuncForStreamTest struct {
}

type mapFuncForStreamTest struct{}

func (tmf *mapFuncForStreamTest) Map(record types.Record) (o types.Record) {
	return record
}

func (trf *reduceFuncForStreamTest) InitialAccmulator() types.Record {
	return types.NewMapRecord(time.Now(), nil)
}

func (trf *reduceFuncForStreamTest) Reduce(a, b types.Record) types.Record {
	return b
}

func Test_All_Stream_Flow(t *testing.T) {

	input1 := make(chan types.Item)
	input2 := make(chan types.Item)
	flow, source := New("test")
	source.Channels(input1, input2).
		SetPartition(4).
		Map(&mapFuncForStreamTest{}).
		Reduce(&reduceFuncForStreamTest{}).
		KeyBy("dimA", "dimB").
		TimeWindow(60)
	expected := 5
	if flow.Length() != expected {
		t.Errorf("flow length wrong got: %v, want: %v", flow.Length(), expected)
	}
	if _, ok := flow.GetStream(0).(*SourceStream); !ok {
		t.Errorf("got unexpected stram type %+v", flow.GetStream(0))
	}
}
