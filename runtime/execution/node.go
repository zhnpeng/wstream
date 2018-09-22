package execution

import (
	"context"
	"sync"

	"github.com/wandouz/wstream/types"
)

// type Node interface {
// 	AddInEdge(inEdge InEdge)
// 	AddOutEdge(outEdge OutEdge)
// 	Run()
// }

type Node struct {
	operator Operator

	in  *Receiver
	out *Emitter

	watermark types.Watermark
	ctx       context.Context
}

func NewNode(in *Receiver, out *Emitter, ctx context.Context) *Node {
	return &Node{
		in:  in,
		out: out,
		ctx: ctx,
	}
}

func (n *Node) Despose() {
	n.out.Despose()
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
	defer n.Despose()
}
