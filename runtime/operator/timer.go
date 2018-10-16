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

func (t *TimerHeapItem) Time() time.Time {
	return t.t
}

func (t *TimerHeapItem) WID() windowing.WindowID {
	return t.wid
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

// ProcessingTimerService processing timer service
type ProcessingTimerService struct {
	handler   TimerHandler
	timerHeap *TimerHeap
	timerMap  map[windowing.WindowID]bool
	ticker    *time.Ticker
	current   time.Time
	mu        sync.Mutex
}

// NewProcessingTimerService bind handler
func NewProcessingTimerService(handler TimerHandler, d time.Duration) *ProcessingTimerService {
	ret := &ProcessingTimerService{
		ticker:    time.NewTicker(d),
		timerHeap: &TimerHeap{},
		timerMap:  make(map[windowing.WindowID]bool),
		handler:   handler,
	}
	ret.Start()
	return ret
}

func (service *ProcessingTimerService) onProcessingTime(t time.Time) {
	service.mu.Lock()
	defer service.mu.Unlock()
	for service.timerHeap.Len() > 0 {
		if service.timerHeap.Top().t.Before(t) {
			item := heap.Pop(service.timerHeap).(TimerHeapItem)
			if _, ok := service.timerMap[item.wid]; ok {
				delete(service.timerMap, item.wid)
			}
			service.handler.onProcessingTime(item.wid, item.t)
		} else {
			break
		}
	}
}

func (service *ProcessingTimerService) RegisterProcessingTimer(wid windowing.WindowID, t time.Time) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if _, ok := service.timerMap[wid]; ok {
		// already registered
		return
	}
	service.timerMap[wid] = true
	heap.Push(service.timerHeap, TimerHeapItem{
		t:   t,
		wid: wid,
	})
}

func (service *ProcessingTimerService) CurrentProcessingTime() time.Time {
	return service.current
}

func (service *ProcessingTimerService) Start() {
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

// EventTimerService is event timer service
// out is used to emit watermark after time drive forward
type EventTimerService struct {
	handler   TimerHandler
	timerHeap *TimerHeap
	timerMap  map[windowing.WindowID]bool
	current   time.Time
	mu        sync.Mutex
}

// NewEventTimerService create new timer service and bind handler to it
func NewEventTimerService(handler TimerHandler) *EventTimerService {
	return &EventTimerService{
		handler:   handler,
		timerHeap: &TimerHeap{},
		timerMap:  make(map[windowing.WindowID]bool),
	}
}

func (service *EventTimerService) Drive(t time.Time) {
	if t.After(service.current) {
		service.onEventTime(t)
	}
}

// CurrentWatermarkTime is window's watermark time or event time
func (service *EventTimerService) CurrentWatermarkTime() time.Time {
	// add 1 second because current time is window's MaxTimestamp which -1 second always
	// FIXME: this is not natural
	return service.current.Add(1 * time.Second)
}

func (service *EventTimerService) RegisterEventTimer(wid windowing.WindowID, t time.Time) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if _, ok := service.timerMap[wid]; ok {
		// already registered
		return
	}
	service.timerMap[wid] = true
	heap.Push(service.timerHeap, TimerHeapItem{
		t:   t,
		wid: wid,
	})
}

func (service *EventTimerService) onEventTime(t time.Time) {
	service.mu.Lock()
	defer service.mu.Unlock()
	for service.timerHeap.Len() > 0 {
		if service.timerHeap.Top().t.Before(t) {
			item := heap.Pop(service.timerHeap).(TimerHeapItem)
			// push event time forward when window's time is after current event time
			if item.Time().After(service.current) {
				service.current = item.Time()
			}
			if _, ok := service.timerMap[item.wid]; ok {
				delete(service.timerMap, item.wid)
			}
			service.handler.onEventTime(item.wid, item.t)
		} else {
			break
		}
	}
}
