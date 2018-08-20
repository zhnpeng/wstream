package execution

import (
	"context"
	"sync"

	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

// PartitionNode emit item to one of its out edges
// according key partition
type PartitionNode struct {
	keys []interface{}

	in  *Receiver
	out *Emitter

	watermark types.Watermark
	ctx       context.Context
}

func (n *PartitionNode) Despose() {
	n.out.Despose()
}

func (n *PartitionNode) AddInEdge(inEdge InEdge) {
	n.in.Add(inEdge)
}

func (n *PartitionNode) AddOutEdge(outEdge OutEdge) {
	n.out.Add(outEdge)
}

func (n *PartitionNode) handleRecord(record types.Record) {
	// get key values, then calculate index, then emit to partition by index
	kvs := record.GetMany(n.keys)
	index := utils.PartitionByKeys(n.out.Length(), kvs)
	n.out.EmitTo(index, record)
}

func (n *PartitionNode) handleWatermark(watermark types.Item) {
	// watermark should always broadcast to all output channels
	n.out.Emit(watermark)
}

func (n *PartitionNode) Run() {
	var wg sync.WaitGroup
	wg.Add(1)
	go n.in.Run()
	go func() {
		defer wg.Done()
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
					// no need to do type assert to watermark because
					// watermark will directly emit to all output channels
					n.handleWatermark(item)
				}
			case <-n.ctx.Done():
				// TODO tell upstream one of its output is closed
				return
			}
		}
	}()
	wg.Wait()
	defer n.Despose()
}
