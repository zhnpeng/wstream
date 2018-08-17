package task

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wandouz/wstream/types"
	"github.com/wandouz/wstream/utils"
)

func TestBroadcastNode_Run(t *testing.T) {
	source := make(Edge)
	sinkB0 := make(Edge)
	sinkB1 := make(Edge)

	nodeA0 := &BroadcastNode{in: NewReceiver(), out: NewEmitter(), ctx: context.Background()}
	nodeB0 := &BroadcastNode{in: NewReceiver(), out: NewEmitter(), ctx: context.Background()}
	nodeB1 := &BroadcastNode{in: NewReceiver(), out: NewEmitter(), ctx: context.Background()}

	edgeA02B0 := make(Edge)
	edgeA02B1 := make(Edge)

	nodeA0.AddInEdge(source.In())
	nodeA0.AddOutEdge(edgeA02B0.Out())
	nodeA0.AddOutEdge(edgeA02B1.Out())

	nodeB0.AddInEdge(edgeA02B0.In())
	nodeB1.AddInEdge(edgeA02B1.In())
	nodeB0.AddOutEdge(sinkB0.Out())
	nodeB1.AddOutEdge(sinkB1.Out())

	tm := utils.TimeParse("2018-08-17 10:00:00")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			item := types.NewWatermark(tm.Add(time.Second))
			source.Out() <- item
		}
		close(source)
	}()

	got := make([]types.Item, 0)
	wg.Add(1)
	go func() {
		defer wg.Done()
		closedChanCnt := 0
		for {
			if closedChanCnt == 2 {
				return
			}
			select {
			case item, ok := <-sinkB0.In():
				if !ok {
					closedChanCnt++
					continue
				}
				got = append(got, item)
				fmt.Println(item)
			case item, ok := <-sinkB1.In():
				if !ok {
					closedChanCnt++
					continue
				}
				got = append(got, item)
				fmt.Println(item)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		nodeA0.Run()
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

	wg.Wait()
	fmt.Println(got)
}
