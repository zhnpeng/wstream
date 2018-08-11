package task

import (
	"context"

	"github.com/wandouz/wstream/streaming/functions"
	"github.com/wandouz/wstream/streaming/sio"
	"github.com/wandouz/wstream/types"
)

type BroadcastNode struct {
	Type      NodeType
	receiver  *Receiver
	emitter   *sio.Emitter
	udf       functions.UserDefinedFunction
	watermark types.Watermark
	ctx       context.Context
}

func (n *BroadcastNode) Despose() {
	n.emitter.Despose()
}

func (n *PartitionNode) handleRecord(record types.Record) {
	if n.udf != nil {
		n.udf.Run(record, n.emitter)
	} else {
		n.emitter.Emit(record)
	}
}

func (n *PartitionNode) handleWatermark(watermark types.Item) {
	// watermark should always broadcast to all output channels
	n.emitter.Emit(watermark)
}

func (n *BroadcastNode) Run() {
	go func() {
		select {
		case item, ok := <-n.receiver.Next():
			if !ok {
				return
			}
			switch item.(type) {
			case types.Record:
				n.handleRecord(item.(types.Record))
			case types.Watermark:
				n.handleWatermark(item)
			}
		case <-n.ctx.Done():
			// TODO tell upstream one of its output is closed
			return
		}
	}()
}
