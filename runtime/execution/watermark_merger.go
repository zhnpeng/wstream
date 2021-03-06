package execution

import (
	"container/heap"
	"time"

	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/types"
)

// WatermarkMerger is serial multiway merger for watermark
type WatermarkMerger struct {
	inputs    []WatermarkChan
	watermark types.Watermark
	output    intfs.Emitter
	wmHeap    *WatermarkHeap
	timeout   time.Duration
}

func NewWatermarkMerger(size int, output intfs.Emitter, timeout time.Duration) *WatermarkMerger {
	inputs := make([]WatermarkChan, size)
	for i := 0; i < size; i++ {
		inputs[i] = make(WatermarkChan, DefaultWatermarkChannelBufferSize)
	}
	return &WatermarkMerger{
		inputs:  inputs,
		output:  output,
		wmHeap:  &WatermarkHeap{},
		timeout: timeout,
	}
}

func (m *WatermarkMerger) Push(id int, wm *types.Watermark) {
	m.inputs[id] <- wm
}

func (m *WatermarkMerger) CloseOne(id int) {
	close(m.inputs[id])
}

func (m *WatermarkMerger) Run() {
	if m.timeout > 0 {
		m.runTimeout()
	} else {
		m.run()
	}
}

func (m *WatermarkMerger) run() {
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

func (m *WatermarkMerger) runTimeout() {
	timer := time.NewTimer(m.timeout)
	defer timer.Stop()
	for {
		for _, ch := range m.inputs {
			select {
			case t := <-timer.C:
				heap.Push(m.wmHeap, WatermarkHeapItem{
					item: types.NewChockWatermark(t),
					ch:   ch,
				})
				timer.Reset(m.timeout)
			case i, ok := <-ch:
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
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(m.timeout)
			}
		}
		for m.wmHeap.Len() > 0 {
			item := heap.Pop(m.wmHeap).(WatermarkHeapItem)
			if item.item.Time().After(m.watermark.Time()) {
				m.output.Emit(item.item)
				m.watermark.T = item.item.Time()
			}
			select {
			case t := <-timer.C:
				heap.Push(m.wmHeap, WatermarkHeapItem{
					item: types.NewChockWatermark(t),
					ch:   item.ch,
				})
				timer.Reset(m.timeout)
			case nextWatermark, ok := <-item.ch:
				if !ok {
					// return if any of input channel is closed
					return
				}
				heap.Push(m.wmHeap, WatermarkHeapItem{
					item: nextWatermark,
					ch:   item.ch,
				})
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(m.timeout)
			}
		}
	}
}
