package task

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wandouz/wstream/types"
	"github.com/wandouz/wstream/utils"
)

func TestBroadcastNode_Run_Single_Source_Watermark_Only(t *testing.T) {
	/*
		Source
			|---> NodeA0
					|---> NodeB0 ---> SinkB0
					|---> NodeB1 ---> SinkB1
	*/
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
			item := types.NewWatermark(tm.Add(time.Duration(i) * time.Second))
			source.Out() <- item
		}
		close(source)
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
	for i, item := range got1 {
		g := item.(*types.Watermark).Time()
		e := tm.Add(time.Duration(i) * time.Second)
		if !g.Equal(e) {
			t.Errorf("got incorrect watermark time got: %v, want: %v", g, e)
		}
	}
	if len(got2) != 50 {
		t.Errorf("got unexcepted count got: %v, want: %v", len(got2), 50)
	}
	for i, item := range got2 {
		g := item.(*types.Watermark).Time()
		e := tm.Add(time.Duration(i) * time.Second)
		if !g.Equal(e) {
			t.Errorf("got incorrect watermark time got: %v, want: %v", g, e)
		}
	}
}

func TestBroadcastNode_Run_Multiple_Source_Watermark_Only(t *testing.T) {
	/*
		Source0
		Source1
		Source2
			|---> NodeA0
					|---> NodeB0 ---> SinkB0
					|---> NodeB1 ---> SinkB1
	*/
	source0 := make(Edge)
	source1 := make(Edge)
	source2 := make(Edge)
	sinkB0 := make(Edge)
	sinkB1 := make(Edge)

	nodeA0 := &BroadcastNode{in: NewReceiver(), out: NewEmitter(), ctx: context.Background()}
	nodeB0 := &BroadcastNode{in: NewReceiver(), out: NewEmitter(), ctx: context.Background()}
	nodeB1 := &BroadcastNode{in: NewReceiver(), out: NewEmitter(), ctx: context.Background()}

	edgeA02B0 := make(Edge)
	edgeA02B1 := make(Edge)

	nodeA0.AddInEdge(source0.In())
	nodeA0.AddInEdge(source1.In())
	nodeA0.AddInEdge(source2.In())
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
			item := types.NewWatermark(tm.Add(time.Duration(i) * time.Second))
			source0.Out() <- item
		}
		close(source0)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			item := types.NewWatermark(tm.Add(time.Duration(i) * time.Second))
			source1.Out() <- item
		}
		close(source1)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			item := types.NewWatermark(tm.Add(time.Duration(i) * time.Second))
			source2.Out() <- item
		}
		close(source2)
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
	for i, item := range got1 {
		g := item.(*types.Watermark).Time()
		e := tm.Add(time.Duration(i) * time.Second)
		if !g.Equal(e) {
			t.Errorf("got incorrect watermark time got: %v, want: %v", g, e)
		}
	}
	if len(got2) != 50 {
		t.Errorf("got unexcepted count got: %v, want: %v", len(got2), 50)
	}
	for i, item := range got2 {
		g := item.(*types.Watermark).Time()
		e := tm.Add(time.Duration(i) * time.Second)
		if !g.Equal(e) {
			t.Errorf("got incorrect watermark time got: %v, want: %v", g, e)
		}
	}
}
