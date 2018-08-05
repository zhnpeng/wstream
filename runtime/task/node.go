package task

import (
	"context"

	"github.com/wandouz/wstream/streaming/functions"
	"github.com/wandouz/wstream/streaming/sio"
	"github.com/wandouz/wstream/types"
)

type Node struct {
	Type      NodeType
	inputs    *Receiver
	outputs   *sio.Emitter
	udf       functions.UserDefinedFunction
	watermark types.Watermark
	ctx       context.Context
}

func (n *Node) Despose() {
	n.outputs.Despose()
}

func (n *Node) Run() {
	go func() {
		select {
		case item, ok := <-n.inputs.Next():
			if !ok {
				return
			}
			if n.udf != nil {
				n.udf.Run(item, n.outputs)
			} else {
				n.outputs.Emit(item)
			}
		case <-n.ctx.Done():
			// TODO tell upstream one of its output is closed
			return
		}
	}()
}
