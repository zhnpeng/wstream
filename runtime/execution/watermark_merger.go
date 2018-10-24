package execution

import (
	"container/heap"

	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/types"
)

// WatermarkMerger is serial multiway merger for watermark
type WatermarkMerger struct {
	inputs    []WatermarkChan
	watermark types.Watermark
	output    intfs.Emitter
	wmHeap    *WatermarkHeap
}

func NewWatermarkMerger(size int, output intfs.Emitter) *WatermarkMerger {
	inputs := make([]WatermarkChan, size)
	for i := 0; i < size; i++ {
		inputs[i] = make(WatermarkChan, DefaultWatermarkChannelBufferSize)
	}
	return &WatermarkMerger{
		inputs: inputs,
		output: output,
		wmHeap: &WatermarkHeap{},
	}
}

func (m *WatermarkMerger) Push(id int, wm *types.Watermark) {
	m.inputs[id] <- wm
}

func (m *WatermarkMerger) CloseOne(id int) {
	close(m.inputs[id])
}

func (m *WatermarkMerger) Run() {
	for {
		for _, ch := range m.inputs {
			i, ok := <-ch
			if !ok {
				/*
					return if any of input channel is closed
					buffer in heap or other channels should not emit
					because they may be disordered becase not all
					channles have data
				*/
				return
			}
			heap.Push(m.wmHeap, WatermarkHeapItem{
				item: i,
				ch:   ch,
			})
		}
		for m.wmHeap.Len() > 0 {
			item := heap.Pop(m.wmHeap).(WatermarkHeapItem)
			if item.item.Time().After(m.watermark.Time()) {
				m.output.Emit(item.item)
				m.watermark.T = item.item.Time()
			}
			nextWatermark, ok := <-item.ch
			if !ok {
				// return if any of input channel is closed
				return
			}
			heap.Push(m.wmHeap, WatermarkHeapItem{
				item: nextWatermark,
				ch:   item.ch,
			})
		}
	}
}
