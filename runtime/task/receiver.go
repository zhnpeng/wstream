package task

import (
	"sync"

	"github.com/wandouz/wstream/types"
)

type WatermarkChan chan *types.Watermark

// Receiver receive items from multiple way
// and do a multi-way merge for watermark
// and parallel by-pass for other items
type Receiver struct {
	wg             *sync.WaitGroup
	mu             sync.Mutex
	inEdges        []InEdge
	watermarkChans []WatermarkChan
	output         Edge
	watermark      types.Watermark
	running        bool
}

func NewReceiver() *Receiver {
	var wg sync.WaitGroup
	return &Receiver{
		output: make(Edge),
		wg:     &wg,
	}
}

func (recv *Receiver) Add(input InEdge) {
	recv.inEdges = append(recv.inEdges, input)
	// watermarkChans map to input inEdges
	recv.watermarkChans = append(recv.watermarkChans, make(WatermarkChan, DefaultWatermarkChannelBufferSize))
}

func (recv *Receiver) Run() {
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
	recv.wg.Add(1)
	go func() {
		defer recv.wg.Done()
		watermarkMerger.Run()
	}()

	//fire up input inEdges
	for id, ch := range recv.inEdges {
		recv.wg.Add(1)
		go func(_id int, _ch InEdge) {
			defer recv.wg.Done()
			for {
				item, ok := <-_ch
				if !ok {
					// TODO: if all inputs are closed close output channel and all watermark inEdges
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
}

func (recv *Receiver) Wait() {
	recv.wg.Wait()
}

// Next will run reciver if it is not running
func (recv *Receiver) Next() <-chan types.Item {
	return recv.output
}
