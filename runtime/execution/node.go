package execution

import (
	"context"
	"sync"

	"github.com/wandouz/wstream/intfs"
)

// type Node interface {
// 	AddInEdge(inEdge InEdge)
// 	AddOutEdge(outEdge OutEdge)
// 	Run()
// }

type Node struct {
	ctx      context.Context
	operator intfs.Operator
	in       *Receiver
	out      *Emitter
}

func NewNode(ctx context.Context, operator intfs.Operator, in *Receiver, out *Emitter) *Node {
	return &Node{
		ctx:      ctx,
		operator: operator,
		in:       in,
		out:      out,
	}
}

func (n *Node) Dispose() {
	n.out.Dispose()
}

func (n *Node) AddInEdge(inEdge InEdge) {
	n.in.Add(inEdge)
}

func (n *Node) AddOutEdge(outEdge OutEdge) {
	n.out.Add(outEdge)
}

func (n *Node) Run() {
	var wg sync.WaitGroup
	wg.Add(1)
	go n.in.Run()
	go func() {
		defer wg.Done()
		// TODO: pass ctx to operator Run
		n.operator.Run(n.in, n.out)
	}()
	wg.Wait()
	defer n.Dispose()
}
