package stream

import (
	"sync"
	"testing"
	"time"

	"github.com/wandouz/wstream/types"
)

func TestStreamGraph_Run(t *testing.T) {
	input1 := make(chan types.Item)
	input2 := make(chan types.Item)
	graph := NewStreamGraph()
	source := NewSourceStream("channels", graph, nil)
	source.Channels(input1, input2).SetPartition(4).
		Map(&testMapFunc{}).
		KeyBy("A", "B").
		Reduce(&testReduceFunc{}).
		Map(&testMapFunc{})
	graph.Transform()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		graph.Run()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Second)
		close(input1)
		close(input2)
	}()
	wg.Wait()
}
