package stream

import (
	"testing"
	"time"

	"github.com/wandouz/wstream/types"
)

type testMapFunc struct{}

func (tmf *testMapFunc) Map(i types.Item) (o types.Item) {
	return i
}

type testReduceFunc struct{}

var testNow = time.Now()

func (trf *testReduceFunc) Accmulator() types.Item {
	return types.NewMapRecord(testNow, nil)
}

func (trf *testReduceFunc) Reduce(a, b types.Item) types.Item {
	return b
}

func Test_All_Stream_Graph(t *testing.T) {
	input1 := make(chan types.Item)
	input2 := make(chan types.Item)
	graph := NewStreamGraph()
	source := NewSourceStream("channels", graph, nil)
	source.Channels(input1, input2).
		SetPartition(4).
		Map(&testMapFunc{}).
		Reduce(&testReduceFunc{}).
		KeyBy("dimA", "dimB").
		TimeWindow(time.Minute)
	expected := 5
	if graph.Length() != expected {
		t.Errorf("graph length wrong got: %v, want: %v", graph.Length(), expected)
	}
	if _, ok := graph.GetStream(0).(*SourceStream); !ok {
		t.Errorf("got unexpected stram type %+v", graph.GetStream(0))
	}
}
