package execution

import (
	"context"
	"sync"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/types"
)

// BroadcastNode emit item to all out edges
type BroadcastNode struct {
	function functions.UserDefinedFunction

	in  *Receiver
	out *Emitter

	watermark types.Watermark
	ctx       context.Context
}

func NewBroadcastNode(in *Receiver, out *Emitter, ctx context.Context) *BroadcastNode {
	return &BroadcastNode{
		in:  in,
		out: out,
		ctx: ctx,
	}
}

func (n *BroadcastNode) Despose() {
	n.out.Despose()
}

func (n *BroadcastNode) AddInEdge(inEdge InEdge) {
	n.in.Add(inEdge)
}

func (n *BroadcastNode) AddOutEdge(outEdge OutEdge) {
	n.out.Add(outEdge)
}

func (n *BroadcastNode) handleRecord(record types.Record) {
	if n.function != nil {
		n.function.Run(record, n.out)
	} else {
		n.out.Emit(record)
	}
}

func (n *BroadcastNode) handleWatermark(watermark types.Item) {
	// watermark should always broadcast to all output channels
	n.out.Emit(watermark)
}

func (n *BroadcastNode) Run() {
	var wg sync.WaitGroup
	wg.Add(1)
	go n.in.Run()
	go func() {
		defer wg.Done()
		for {
			select {
			case item, ok := <-n.in.Next():
				if !ok {
					return
				}
				switch item.(type) {
				case types.Record:
					n.handleRecord(item.(types.Record))
				case *types.Watermark:
					// no need to do type assert to watermark because
					// watermark will directly emit to all output channels
					n.handleWatermark(item)
				}
			case <-n.ctx.Done():
				// TODO tell upstream one of its output is closed
				return
			}
		}
	}()
	wg.Wait()
	defer n.Despose()
}
