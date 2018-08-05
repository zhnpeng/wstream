package task

import (
	"sync"

	"github.com/wandouz/wstream/types"
)

type WatermarkChan chan *types.Watermark

type Receiver struct {
	channels   []types.ItemChan
	wmChannels []WatermarkChan
	output     types.ItemChan
	watermark  types.Watermark
	running    bool
	mu         sync.Mutex
}

func (recv *Receiver) AddInput(input types.ItemChan) {
	recv.channels = append(recv.channels, input)
	// wmChannels map to input channels
	recv.wmChannels = append(recv.wmChannels, make(WatermarkChan))
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

	var wg sync.WaitGroup
	//fire up watermakr merger
	watermarkMerger := NewWatermarkMerger(recv.wmChannels, recv.output)
	wg.Add(1)
	go func() {
		defer wg.Done()
		watermarkMerger.Run()
	}()

	//fire up input channels
	for id, ch := range recv.channels {
		wg.Add(1)
		go func(_id int, _ch types.ItemChan) {
			defer wg.Done()
			item, ok := <-_ch
			if !ok {
				// TODO: if all inputs are closed close output channel and all watermark channels
				close(recv.wmChannels[_id])
				return
			}
			switch item.(type) {
			case *types.Watermark:
				// multiway merge watermark from multiple inputs to one
				recv.wmChannels[_id] <- item.(*types.Watermark)
			default:
				recv.output <- item
			}
		}(id, ch)
	}
	wg.Wait()
}

func (recv *Receiver) Next() <-chan types.Item {
	if !recv.running {
		go recv.Run()
	}
	return recv.output
}
