package task

import (
	"context"

	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/streaming/sio"
	"github.com/wandouz/wstream/types"
)

type PartitionNode struct {
	Type      NodeType
	receiver  *Receiver
	emitter   *sio.Emitter
	watermark types.Watermark
	ctx       context.Context

	keys []interface{}
}

func (n *PartitionNode) Despose() {
	n.emitter.Despose()
}

func (n *PartitionNode) handleRecord(record types.Record) {
	// get key values, then calculate index, then emit to partition by index
	kvs := item.GetMany(n.keys)
	index := utils.PartitionByKeys(n.emitter.Length(), kvs)
	n.emitter.EmitTo(index, item)
}

func (n *PartitionNode) handleWatermark(watermark types.Item) {
	// watermark should always broadcast to all output channels
	n.emitter.Emit(watermark)
}

func (n *PartitionNode) Run() {
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
				// no need to do type assert to watermark because
				// watermark will directly emit to all output channels
				n.handleWatermark(item)
			}
		case <-n.ctx.Done():
			// TODO tell upstream one of its output is closed
			return
		}
	}()
}
