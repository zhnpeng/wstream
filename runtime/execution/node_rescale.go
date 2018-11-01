package execution

import (
	"context"
	"sync"

	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/types"
)

type RescaleNode struct {
	ctx      context.Context
	mu       sync.Mutex
	inputs   []InEdge
	out      *Emitter
	selector intfs.Selector
	running  bool
}

func NewRescaleNode(ctx context.Context, selector intfs.Selector) *RescaleNode {
	return &RescaleNode{
		ctx:      ctx,
		selector: selector,
		out:      NewEmitter(),
	}
}

func (n *RescaleNode) AddInEdge(in InEdge) {
	n.inputs = append(n.inputs, in)
}

func (n *RescaleNode) AddInEdges(ins ...InEdge) {
	n.inputs = append(n.inputs, ins...)
}

func (n *RescaleNode) AddOutEdge(out OutEdge) {
	n.out.Add(out)
}

func (n *RescaleNode) AddOutEdges(outs ...OutEdge) {
	n.out.Adds(outs...)
}

func (n *RescaleNode) Run() {
	var wg sync.WaitGroup

	if n.running {
		return
	}
	n.mu.Lock()
	if n.running {
		return
	}
	n.running = true
	n.mu.Unlock()

	merger := NewWatermarkMerger(len(n.inputs), n.out)
	wg.Add(1)
	go func() {
		defer wg.Done()
		merger.Run()
	}()

	for i, ch := range n.inputs {
		wg.Add(1)
		go func(_i int, _ch InEdge) {
			defer wg.Done()
			for {
				item, ok := <-_ch
				if !ok {
					merger.CloseOne(_i)
					return
				}
				switch item.(type) {
				case *types.Watermark:
					merger.Push(_i, item.(*types.Watermark))
				default:
					index := n.selector.Select(item.(types.Record), n.out.Length())
					n.out.EmitTo(index, item)
				}
			}
		}(i, ch)
	}
	wg.Wait()
	defer n.out.Dispose()
}
