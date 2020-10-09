package execution

import (
	"context"
	"fmt"
	"sync"

	"github.com/zhnpeng/wstream/intfs"
)

type Node interface {
	AddInEdge(InEdge)
	AddOutEdge(OutEdge)
	Run()
}

type ExecutionNode struct {
	ctx      context.Context
	in       *Receiver
	out      *Emitter
	operator intfs.Operator
}

func NewExecutionNode(ctx context.Context, operator intfs.Operator) *ExecutionNode {
	return &ExecutionNode{
		ctx:      ctx,
		operator: operator,
		in:       NewReceiver(),
		out:      NewEmitter(),
	}
}

func (n *ExecutionNode) AddInEdge(ie InEdge) {
	n.in.Add(ie)
}

func (n *ExecutionNode) AddInEdges(ies ...InEdge) {
	n.in.Adds(ies...)
}

func (n *ExecutionNode) AddOutEdge(out OutEdge) {
	n.out.Add(out)
}

func (n *ExecutionNode) AddOutEdges(outs ...OutEdge) {
	n.out.Adds(outs...)
}

func (n *ExecutionNode) String() string {
	return fmt.Sprintf("in: %v, out: %v", n.in, n.out)
}

func (n *ExecutionNode) Run() {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		n.in.Run()
	}()
	// TODO: pass ctx to operator Run
	n.operator.Run(n.in, n.out)
	wg.Wait()
}
