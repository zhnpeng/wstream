package execution

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wandouz/wstream/runtime/selector"
	"github.com/wandouz/wstream/types"
	"github.com/wandouz/wstream/utils"
)

func TestRescaleNode_Run(t *testing.T) {
	source0 := make(Edge)
	source1 := make(Edge)
	source2 := make(Edge)
	sink0 := make(Edge)
	sink1 := make(Edge)

	node := NewRescaleNode(context.Background(), selector.NewRoundRobinSelector())
	node.AddInEdges(source0.In(), source1.In(), source2.In())
	node.AddOutEdges(sink0.Out(), sink1.Out())
	tm := utils.ParseTime("2018-08-17 10:00:00")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			item := types.NewMapRecord(
				tm.Add(time.Duration(i)*time.Second),
				map[string]interface{}{
					"X": i,
				},
			)
			source0.Out() <- item
		}
		close(source0)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			item := types.NewMapRecord(
				tm.Add(time.Duration(i)*time.Second),
				map[string]interface{}{
					"X": i,
				},
			)
			source1.Out() <- item
		}
		close(source1)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			item := types.NewMapRecord(
				tm.Add(time.Duration(i)*time.Second),
				map[string]interface{}{
					"X": i,
				},
			)
			source2.Out() <- item
		}
		close(source2)
	}()

	got0 := make([]types.Item, 0)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			item, ok := <-sink0.In()
			if !ok {
				return
			}
			got0 = append(got0, item)
		}
	}()

	got1 := make([]types.Item, 0)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			item, ok := <-sink1.In()
			if !ok {
				return
			}
			got1 = append(got1, item)
		}
	}()

	node.Run()
	wg.Wait()
	if len(got0) != 4 {
		t.Errorf("got unexpected count got: %v, want: %v", len(got0), 4)
	}
	if len(got1) != 5 {
		t.Errorf("got unexpected count got: %v, want: %v", len(got1), 5)
	}
}
