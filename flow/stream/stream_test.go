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
		Map(&testMapFunc{}).
		SetPartition(4).
		Reduce(&testReduceFunc{}).
		KeyBy("dimA", "dimB").
		TimeWindow(time.Minute)
	excepted := 5
	if graph.Length() != excepted {
		t.Errorf("graph length wrong got: %v, want: %v", graph.Length(), excepted)
	}
}
