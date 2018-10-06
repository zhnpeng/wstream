package operator

import (
	"container/heap"
	"sync"
	"time"
)

type TimerHandler interface {
	onProcessingTime(time time.Time)
	onEventTime(time time.Time)
}

type TimerHeapItem struct {
	T       time.Time
	Handler TimerHandler
}

type TimerHeap []TimerHeapItem

func (h TimerHeap) Len() int { return len(h) }

// Less function for heap sort by Time
func (h TimerHeap) Less(i, j int) bool {
	return h[i].T.Before(h[j].T)
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
	timerHeap   *TimerHeap
	timerExists map[time.Time]bool
	ticker      *time.Ticker
	mu          sync.Mutex
}

func NewProcessingTimerService(d time.Duration) *ProcessingTimerService {
	ret := &ProcessingTimerService{
		ticker:      time.NewTicker(d),
		timerExists: make(map[time.Time]bool),
	}
	ret.start()
	return ret
}

func (service *ProcessingTimerService) onProcessingTime(t time.Time) {
	service.mu.Lock()
	defer service.mu.Unlock()
	for service.timerHeap.Top().T.Before(t) {
		item := heap.Pop(service.timerHeap).(TimerHeapItem)
		if _, ok := service.timerExists[item.T]; ok {
			delete(service.timerExists, item.T)
		}
		item.Handler.onProcessingTime(item.T)
	}
}

func (service *ProcessingTimerService) RegisterProcessingTimer(t time.Time, handler TimerHandler) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if _, ok := service.timerExists[t]; ok {
		// already registered
		return
	}
	heap.Push(service.timerHeap, &TimerHeapItem{
		T:       t,
		Handler: handler,
	})
}

func (service *ProcessingTimerService) start() {
	go func() {
		for t := range service.ticker.C {
			service.onProcessingTime(t)
		}
	}()
}

func (service *ProcessingTimerService) Stop() {
	service.ticker.Stop()
}

type EventTimerService struct {
	timerHeap        *TimerHeap
	timerExists      map[time.Time]bool
	currentEventTime time.Time
	mu               sync.Mutex
}

func NewEventTimerService() *EventTimerService {
	return &EventTimerService{
		timerExists: make(map[time.Time]bool),
	}
}

func (service *EventTimerService) Drive(t time.Time) {
	if t.After(service.currentEventTime) {
		service.currentEventTime = t
		service.onEventTime(t)
	}
}

func (service *EventTimerService) CurrentEventTime() time.Time {
	return service.currentEventTime
}

func (service *EventTimerService) RegisterEventTimer(t time.Time, handler TimerHandler) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if _, ok := service.timerExists[t]; ok {
		// already registered
		return
	}
	heap.Push(service.timerHeap, &TimerHeapItem{
		T:       t,
		Handler: handler,
	})
}

func (service *EventTimerService) onEventTime(t time.Time) {
	service.mu.Lock()
	defer service.mu.Unlock()
	for service.timerHeap.Top().T.Before(t) {
		item := heap.Pop(service.timerHeap).(TimerHeapItem)
		if _, ok := service.timerExists[item.T]; ok {
			delete(service.timerExists, item.T)
		}
		item.Handler.onEventTime(item.T)
	}
}
