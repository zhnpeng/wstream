package execution

import (
	"sync"

	"github.com/zhnpeng/wstream/types"
)

type WatermarkChan chan *types.Watermark

// Receiver receive items from multiple way
// and do a multi-way merge for watermark
// and parallel by-pass for other items
type Receiver struct {
	mu      sync.Mutex
	inEdges []InEdge
	output  Edge
	running bool
}

func NewReceiver() *Receiver {
	return &Receiver{
		output: make(Edge),
	}
}

func (recv *Receiver) Add(input InEdge) {
	recv.inEdges = append(recv.inEdges, input)
}

func (recv *Receiver) Adds(inputs ...InEdge) {
	recv.inEdges = append(recv.inEdges, inputs...)
}

func (recv *Receiver) Run() {
	var wg sync.WaitGroup
	if recv.running {
		return
	}
	recv.mu.Lock()
	if recv.running {
		return
	}
	recv.running = true
	recv.mu.Unlock()

	emitter := NewSingleEmitter(recv.output)
	merger := NewWatermarkMerger(len(recv.inEdges), emitter, 0)
	//fire up watermakr merger
	wg.Add(1)
	go func() {
		defer wg.Done()
		merger.Run()
	}()

	//fire up input inEdges
	for id, ch := range recv.inEdges {
		wg.Add(1)
		go func(_id int, _ch InEdge) {
			defer wg.Done()
			for {
				item, ok := <-_ch
				if !ok {
					merger.CloseOne(_id)
					return
				}
				switch item.(type) {
				case *types.Watermark:
					// multiway merge watermark from multiple inputs to one
					merger.Push(_id, item.(*types.Watermark))
				default:
					recv.output <- item
				}
			}
		}(id, ch)
	}
	wg.Wait()
	// dispose self before return
	defer recv.Dispose()
}

func (recv *Receiver) Dispose() {
	close(recv.output)
}

// Next will run reciver if it is not running
func (recv *Receiver) Next() <-chan types.Item {
	return recv.output
}
