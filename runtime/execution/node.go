package execution

import (
	"context"
	"sync"

	"github.com/wandouz/wstream/types"
)

type Node interface {
	AddInEdge(inEdge InEdge)
	AddOutEdge(outEdge OutEdge)
	Run()
}

type ExecutionNode struct {
	operator Operator

	in  *Receiver
	out *Emitter

	watermark types.Watermark
	ctx       context.Context
}

func NewExecutionNode(in *Receiver, out *Emitter, ctx context.Context) *ExecutionNode {
	return &ExecutionNode{
		in:  in,
		out: out,
		ctx: ctx,
	}
}

func (n *ExecutionNode) Despose() {
	n.out.Despose()
}

func (n *ExecutionNode) AddInEdge(inEdge InEdge) {
	n.in.Add(inEdge)
}

func (n *ExecutionNode) AddOutEdge(outEdge OutEdge) {
	n.out.Add(outEdge)
}

func (n *ExecutionNode) Run() {
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
