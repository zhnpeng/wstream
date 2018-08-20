package execution

import "github.com/wandouz/wstream/types"

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
