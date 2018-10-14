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

func Test_All_Stream_Graph(t *testing.T) {

	input1 := make(chan types.Item)
	input2 := make(chan types.Item)
	graph := NewStreamGraph()
	source := NewSourceStream("channels", graph)
	source.Channels(input1, input2).
		SetPartition(4).
		Map(&mapFuncForStreamTest{}).
		Reduce(&reduceFuncForStreamTest{}).
		KeyBy("dimA", "dimB").
		TimeWindow(60)
	expected := 5
	if graph.Length() != expected {
		t.Errorf("graph length wrong got: %v, want: %v", graph.Length(), expected)
	}
	if _, ok := graph.GetStream(0).(*SourceStream); !ok {
		t.Errorf("got unexpected stram type %+v", graph.GetStream(0))
	}
}
