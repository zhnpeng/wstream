package task

import (
	"sync"

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

func (h *WatermarkHeap) Push(x WatermarkHeapItem) {
	*h = append(*h, WatermarkHeapItem(x))
}

func (h *WatermarkHeap) Pop() WatermarkHeapItem {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// WatermarkMerger is multiway merger for watermark
type WatermarkMerger struct {
	inputs []WatermarkChan
	closed map[int]bool
	output types.ItemChan
	hp     *WatermarkHeap
	mu     sync.Mutex
}

func NewWatermarkMerger(inputs []WatermarkChan, output types.ItemChan) *WatermarkMerger {
	closed := make(map[int]bool, 0)
	for id := range inputs {
		closed[id] = false
	}
	return &WatermarkMerger{
		inputs: inputs,
		output: output,
		closed: closed,
		hp:     &WatermarkHeap{},
	}
}

func (m *WatermarkMerger) Close(id int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed[id] = true
	for _, isClosed := range m.closed {
		if isClosed == false {
			return false
		}
	}
	return true
}

func (m *WatermarkMerger) Run() {
	for {
		for id, ch := range m.inputs {
			i, ok := <-ch
			if !ok {
				if m.Close(id) {
					return
				}
				continue
			}
			m.hp.Push(WatermarkHeapItem{
				item: i,
				ch:   ch,
			})
		}
		for m.hp.Len() > 0 {
			item := m.hp.Pop()
			m.output <- item.item
			nextWatermark, ok := <-item.ch
			if !ok {
				continue
			}
			m.hp.Push(WatermarkHeapItem{
				item: nextWatermark,
				ch:   item.ch,
			})
		}
	}
}
