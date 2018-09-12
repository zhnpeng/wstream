package execution

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wandouz/wstream/helpers"
	"github.com/wandouz/wstream/types"
)

func TestRoundRobinNode_Run(t *testing.T) {
	source := make(Edge)
	nodeA := &RoundRobinNode{in: NewReceiver(), out: NewEmitter(), ctx: context.Background()}
	nodeB0 := &BroadcastNode{in: NewReceiver(), out: NewEmitter(), ctx: context.Background()}
	nodeB1 := &BroadcastNode{in: NewReceiver(), out: NewEmitter(), ctx: context.Background()}

	edgeA2B0 := make(Edge)
	edgeA2B1 := make(Edge)

	nodeA.AddInEdge(source.In())
	nodeA.AddOutEdge(edgeA2B0.Out())
	nodeA.AddOutEdge(edgeA2B1.Out())

	nodeB0.AddInEdge(edgeA2B0.In())
	nodeB1.AddInEdge(edgeA2B1.In())

	sinkB0 := make(Edge)
	sinkB1 := make(Edge)
	nodeB0.AddOutEdge(sinkB0.Out())
	nodeB1.AddOutEdge(sinkB1.Out())

	tm := helpers.TimeParse("2018-08-17 10:00:00")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			item := types.NewWatermark(tm.Add(time.Duration(i) * time.Second))
			source.Out() <- item
		}
		close(source)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		nodeA.Run()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		nodeB0.Run()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		nodeB1.Run()
	}()

	got1 := make([]types.Item, 0)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			item, ok := <-sinkB0.In()
			if !ok {
				return
			}
			got1 = append(got1, item)
		}
	}()

	got2 := make([]types.Item, 0)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			item, ok := <-sinkB1.In()
			if !ok {
				return
			}
			got2 = append(got2, item)
		}
	}()

	wg.Wait()
	if len(got1) != 50 {
		t.Errorf("got unexcepted count got: %v, want: %v", len(got1), 50)
	}
	if len(got2) != 50 {
		t.Errorf("got unexcepted count got: %v, want: %v", len(got2), 50)
	}
}
