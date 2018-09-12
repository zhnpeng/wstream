package execution

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/wandouz/wstream/types"
)

// RoundRobinNode emit item to one of its out edges
// according key partition
type RoundRobinNode struct {
	in  *Receiver
	out *Emitter

	watermark types.Watermark
	ctx       context.Context

	count int32
}

func NewRoundRobinNode(in *Receiver, out *Emitter, ctx context.Context) *RoundRobinNode {
	return &RoundRobinNode{
		in:  in,
		out: out,
		ctx: ctx,
	}
}

func (n *RoundRobinNode) Despose() {
	n.out.Despose()
}

func (n *RoundRobinNode) AddInEdge(inEdge InEdge) {
	n.in.Add(inEdge)
}

func (n *RoundRobinNode) AddOutEdge(outEdge OutEdge) {
	n.out.Add(outEdge)
}

func (n *RoundRobinNode) handleRecord(record types.Record) {
	// get key values, then calculate index, then emit to partition by index
	cnt := atomic.AddInt32(&n.count, 1)
	index := cnt % int32(n.out.Length())
	n.out.EmitTo(int(index), record)
}

func (n *RoundRobinNode) handleWatermark(watermark types.Item) {
	// watermark should always broadcast to all output channels
	n.out.Emit(watermark)
}

func (n *RoundRobinNode) Run() {
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
