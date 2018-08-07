package task

import (
	"sync"

	"github.com/wandouz/wstream/types"
)

type WatermarkChan chan *types.Watermark

type Receiver struct {
	wg             *sync.WaitGroup
	mu             sync.Mutex
	channels       []types.ItemChan
	watermarkChans []WatermarkChan
	output         types.ItemChan
	watermark      types.Watermark
	running        bool
}

func NewReceiver() *Receiver {
	var wg sync.WaitGroup
	return &Receiver{
		output: make(types.ItemChan),
		wg:     &wg,
	}
}

func (recv *Receiver) AddInput(input types.ItemChan) {
	recv.channels = append(recv.channels, input)
	// watermarkChans map to input channels
	recv.watermarkChans = append(recv.watermarkChans, make(WatermarkChan, 300))
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

	//fire up input channels
	for id, ch := range recv.channels {
		recv.wg.Add(1)
		go func(_id int, _ch types.ItemChan) {
			defer recv.wg.Done()
			for {
				item, ok := <-_ch
				if !ok {
					// TODO: if all inputs are closed close output channel and all watermark channels
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

func (recv *Receiver) Next() <-chan types.Item {
	if !recv.running {
		go recv.Run()
	}
	return recv.output
}