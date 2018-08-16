package task

import (
	"context"

	"github.com/wandouz/wstream/streaming/functions"
	"github.com/wandouz/wstream/types"
)

type BroadcastNode struct {
	udf functions.UserDefinedFunction

	in  *Receiver
	out *Emitter

	Type      NodeType
	watermark types.Watermark
	ctx       context.Context
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
	if n.udf != nil {
		n.udf.Run(record, n.out)
	} else {
		n.out.Emit(record)
	}
}

func (n *BroadcastNode) handleWatermark(watermark types.Item) {
	// watermark should always broadcast to all output channels
	n.out.Emit(watermark)
}

func (n *BroadcastNode) Run() {
	go func() {
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
					n.handleWatermark(item)
				}
			case <-n.ctx.Done():
				// TODO tell upstream one of its output is closed
				return
			}
		}
	}()
}
