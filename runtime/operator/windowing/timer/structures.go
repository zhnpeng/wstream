package timer

import (
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing"
)

type TimerHandler interface {
	OnProcessingTime(windowing.WindowID, time.Time)
	OnEventTime(windowing.WindowID, time.Time)
	Dispose()
}

type TimerHeap []TimerHeapItem

type TimerHeapItem struct {
	t   time.Time
	wid windowing.WindowID
}

func (t *TimerHeapItem) Time() time.Time {
	return t.t
}

func (t *TimerHeapItem) WID() windowing.WindowID {
	return t.wid
}

func (h TimerHeap) Len() int { return len(h) }

// Less function for heap sort by Time
func (h TimerHeap) Less(i, j int) bool {
	return h[i].t.Before(h[j].t)
}

func (h TimerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h TimerHeap) Top() TimerHeapItem {
	return h[0]
}

func (h *TimerHeap) Push(x interface{}) {
	*h = append(*h, x.(TimerHeapItem))
}

func (h *TimerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
