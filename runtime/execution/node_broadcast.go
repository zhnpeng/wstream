package execution

import (
	"context"
	"sync"

	"github.com/wandouz/wstream/types"
)

// BroadcastNode emit item to all out edges
type BroadcastNode struct {
	operator Operator

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

func (n *BroadcastNode) Run() {
	var wg sync.WaitGroup
	wg.Add(1)
	go n.in.Run()
	go func() {
		defer wg.Done()
		// TODO: pass ctx to operator Run
		n.operator.Run(n.in, n.out)
	}()
	wg.Wait()
	defer n.Despose()
}
