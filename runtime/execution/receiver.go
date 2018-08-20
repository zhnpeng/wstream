package execution

import (
	"sync"

	"github.com/wandouz/wstream/types"
)

type WatermarkChan chan *types.Watermark

// Receiver receive items from multiple way
// and do a multi-way merge for watermark
// and parallel by-pass for other items
type Receiver struct {
	mu             sync.Mutex
	inEdges        []InEdge
	watermarkChans []WatermarkChan
	output         Edge
	watermark      types.Watermark
	running        bool
}

func NewReceiver() *Receiver {
	return &Receiver{
		output: make(Edge),
	}
}

func (recv *Receiver) Add(input InEdge) {
	recv.inEdges = append(recv.inEdges, input)
	// watermarkChans map to input inEdges
	recv.watermarkChans = append(recv.watermarkChans, make(WatermarkChan, DefaultWatermarkChannelBufferSize))
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

	//fire up watermakr merger
	watermarkMerger := NewWatermarkMerger(recv.watermarkChans, recv.output)
	wg.Add(1)
	go func() {
		defer wg.Done()
		watermarkMerger.Run()
	}()

	//fire up input inEdges
	for id, ch := range recv.inEdges {
		wg.Add(1)
		go func(_id int, _ch InEdge) {
			defer wg.Done()
			for {
				item, ok := <-_ch
				if !ok {
					close(recv.watermarkChans[_id])
					return
				}
				switch item.(type) {
				case *types.Watermark:
					// multiway merge watermark from multiple inputs to one
					recv.watermarkChans[_id] <- item.(*types.Watermark)
				default:
					recv.output <- item
				}
			}
		}(id, ch)
	}
	wg.Wait()
	// despose self before return
	defer recv.Despose()
}

func (recv *Receiver) Despose() {
	close(recv.output)
}

// Next will run reciver if it is not running
func (recv *Receiver) Next() <-chan types.Item {
	return recv.output
}
