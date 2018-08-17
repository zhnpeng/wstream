package task

import (
	"container/heap"

	"github.com/wandouz/wstream/types"
)

type WatermarkHeapItem struct {
	item *types.Watermark
	ch   WatermarkChan
}

type WatermarkHeap []WatermarkHeapItem

func (h WatermarkHeap) Len() int { return len(h) }

// Less function for heap sort by Time
func (h WatermarkHeap) Less(i, j int) bool {
	return h[i].item.Time().Before(h[j].item.Time())
}

func (h WatermarkHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h WatermarkHeap) Top() WatermarkHeapItem {
	return h[0]
}

func (h *WatermarkHeap) Push(x interface{}) {
	*h = append(*h, x.(WatermarkHeapItem))
}

func (h *WatermarkHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// WatermarkMerger is serial multiway merger for watermark
type WatermarkMerger struct {
	inputs    []WatermarkChan
	watermark types.Watermark
	output    Edge
	wmHeap    *WatermarkHeap
}

func NewWatermarkMerger(inputs []WatermarkChan, output Edge) *WatermarkMerger {
	return &WatermarkMerger{
		inputs: inputs,
		output: output,
		wmHeap: &WatermarkHeap{},
	}
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
				m.output <- item.item
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
