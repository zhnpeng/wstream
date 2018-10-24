package execution

import (
	"context"
	"sync"

	"github.com/wandouz/wstream/intfs"
)

// type BroadcastNode interface {
// 	AddInEdge(inEdge InEdge)
// 	AddOutEdge(outEdge OutEdge)
// 	Run()
// }

type BroadcastNode struct {
	ctx      context.Context
	operator intfs.Operator
	in       *Receiver
	out      *Emitter
}

func NewBroadcastNode(ctx context.Context, operator intfs.Operator, in *Receiver, out *Emitter) *BroadcastNode {
	return &BroadcastNode{
		ctx:      ctx,
		operator: operator,
		in:       in,
		out:      out,
	}
}

func (n *BroadcastNode) Dispose() {
	n.out.Dispose()
}

func (n *BroadcastNode) AddInEdge(inEdge InEdge) {
	n.in.Add(inEdge)
}

func (n *BroadcastNode) AddOutEdge(outEdge OutEdge) {
	n.out.Add(outEdge)
}

func (n *BroadcastNode) Run() {
	var wg sync.WaitGroup
	go n.in.Run()
	wg.Add(1)
	go func() {
		defer wg.Done()
		// TODO: pass ctx to operator Run
		n.operator.Run(n.in, n.out)
	}()
	wg.Wait()
	defer n.Dispose()
}
