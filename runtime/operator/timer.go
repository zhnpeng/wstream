package operator

import (
	"container/heap"
	"sync"
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing"
)

type TimerHandler interface {
	onProcessingTime(windowing.WindowID, time.Time)
	onEventTime(windowing.WindowID, time.Time)
}

type TimerHeapItem struct {
	t   time.Time
	wid windowing.WindowID
}

type TimerHeap []TimerHeapItem

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

type ProcessingTimerService struct {
	handler     TimerHandler
	timerHeap   *TimerHeap
	timerExists map[windowing.WindowID]bool
	ticker      *time.Ticker
	current     time.Time
	mu          sync.Mutex
}

// NewProcessingTimerService bind handler
func NewProcessingTimerService(handler TimerHandler, d time.Duration) *ProcessingTimerService {
	ret := &ProcessingTimerService{
		ticker:      time.NewTicker(d),
		timerExists: make(map[windowing.WindowID]bool),
		handler:     handler,
	}
	ret.start()
	return ret
}

func (service *ProcessingTimerService) onProcessingTime(t time.Time) {
	service.mu.Lock()
	defer service.mu.Unlock()
	for service.timerHeap.Top().t.Before(t) {
		item := heap.Pop(service.timerHeap).(TimerHeapItem)
		if _, ok := service.timerExists[item.wid]; ok {
			delete(service.timerExists, item.wid)
		}
		service.handler.onProcessingTime(item.wid, item.t)
	}
}

func (service *ProcessingTimerService) RegisterProcessingTimer(wid windowing.WindowID, t time.Time) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if _, ok := service.timerExists[wid]; ok {
		// already registered
		return
	}
	heap.Push(service.timerHeap, &TimerHeapItem{
		t:   t,
		wid: wid,
	})
}

func (service *ProcessingTimerService) CurrentProcessingTime() time.Time {
	return service.current
}

func (service *ProcessingTimerService) start() {
	go func() {
		for t := range service.ticker.C {
			service.current = t
			service.onProcessingTime(t)
		}
	}()
}

func (service *ProcessingTimerService) Stop() {
	service.ticker.Stop()
}

type EventTimerService struct {
	handler     TimerHandler
	timerHeap   *TimerHeap
	timerExists map[time.Time]bool
	current     time.Time
	mu          sync.Mutex
}

// NewEventTimerService create new timer service and bind handler to it
func NewEventTimerService(handler TimerHandler) *EventTimerService {
	return &EventTimerService{
		handler:     handler,
		timerExists: make(map[time.Time]bool),
	}
}

func (service *EventTimerService) Drive(t time.Time) {
	if t.After(service.current) {
		service.current = t
		service.onEventTime(t)
	}
}

func (service *EventTimerService) CurrentEventTime() time.Time {
	return service.current
}

func (service *EventTimerService) RegisterEventTimer(wid windowing.WindowID, t time.Time) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if _, ok := service.timerExists[t]; ok {
		// already registered
		return
	}
	heap.Push(service.timerHeap, &TimerHeapItem{
		t:   t,
		wid: wid,
	})
}

func (service *EventTimerService) onEventTime(t time.Time) {
	service.mu.Lock()
	defer service.mu.Unlock()
	for service.timerHeap.Top().t.Before(t) {
		item := heap.Pop(service.timerHeap).(TimerHeapItem)
		if _, ok := service.timerExists[item.t]; ok {
			delete(service.timerExists, item.t)
		}
		service.handler.onEventTime(item.wid, item.t)
	}
}
