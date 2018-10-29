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
	Dispose()
}

// WindowTimerService processing timer service
type WindowTimerService struct {
	handler TimerHandler
	stoped  chan struct{}

	procHp     *TimerHeap
	procExists map[windowing.WindowID]bool
	procTicker *time.Ticker
	procCurrT  time.Time
	procMu     sync.Mutex
	procMupt   sync.RWMutex

	eventHp     *TimerHeap
	eventExists map[windowing.WindowID]bool
	eventCurrT  time.Time
	eventMu     sync.Mutex
}

// NewWindowTimerService bind handler
func NewWindowTimerService(handler TimerHandler, d time.Duration) *WindowTimerService {
	ret := &WindowTimerService{
		handler: handler,
		stoped:  make(chan struct{}),

		procHp:     &TimerHeap{},
		procExists: make(map[windowing.WindowID]bool),
		procTicker: time.NewTicker(d),
		procCurrT:  time.Now(),

		eventHp:     &TimerHeap{},
		eventExists: make(map[windowing.WindowID]bool),
	}
	return ret
}

func (service *WindowTimerService) onProcessingTime(t time.Time) {
	service.procMu.Lock()
	defer service.procMu.Unlock()
	for service.procHp.Len() > 0 {
		if service.procHp.Top().t.Before(t) {
			item := heap.Pop(service.procHp).(TimerHeapItem)
			if _, ok := service.procExists[item.wid]; ok {
				delete(service.procExists, item.wid)
			}
			service.handler.onProcessingTime(item.wid, item.t)
		} else {
			break
		}
	}
}

func (service *WindowTimerService) RegisterProcessingTimer(wid windowing.WindowID, t time.Time) {
	service.procMu.Lock()
	defer service.procMu.Unlock()
	if _, ok := service.procExists[wid]; ok {
		// already registered
		return
	}
	service.procExists[wid] = true
	heap.Push(service.procHp, TimerHeapItem{
		t:   t,
		wid: wid,
	})
}

func (service *WindowTimerService) CurrentProcessingTime() time.Time {
	service.procMupt.RLock()
	defer service.procMupt.RUnlock()
	return service.procCurrT
}

func (service *WindowTimerService) SetProcessingTime(t time.Time) {
	service.procMupt.Lock()
	defer service.procMupt.Unlock()
	service.procCurrT = t
}

func (service *WindowTimerService) Start() {
	go func() {
		defer service.procTicker.Stop()
		for {
			select {
			case t := <-service.procTicker.C:
				t = t.Truncate(time.Second)
				service.SetProcessingTime(t)
				service.onProcessingTime(t)
			case <-service.stoped:
				service.handler.Dispose()
				return
			}
		}
	}()
}

func (service *WindowTimerService) Stop() {
	close(service.stoped)
}

func (service *WindowTimerService) Drive(t time.Time) {
	if t.After(service.eventCurrT) {
		service.onEventTime(t)
	}
}

// CurrentWatermarkTime is window's watermark time or event time
func (service *WindowTimerService) CurrentWatermarkTime() time.Time {
	return service.eventCurrT
}

func (service *WindowTimerService) RegisterEventTimer(wid windowing.WindowID, t time.Time) {
	service.eventMu.Lock()
	defer service.eventMu.Unlock()
	if _, ok := service.eventExists[wid]; ok {
		// already registered
		return
	}
	service.eventExists[wid] = true
	heap.Push(service.eventHp, TimerHeapItem{
		t:   t,
		wid: wid,
	})
}

func (service *WindowTimerService) onEventTime(t time.Time) {
	service.eventMu.Lock()
	defer service.eventMu.Unlock()
	for service.eventHp.Len() > 0 {
		if service.eventHp.Top().t.Before(t) {
			item := heap.Pop(service.eventHp).(TimerHeapItem)
			// push event time forward when window's time is after current event time
			start := item.wid.Window().Start()
			if start.After(service.eventCurrT) {
				service.eventCurrT = start
			}
			if _, ok := service.eventExists[item.wid]; ok {
				delete(service.eventExists, item.wid)
			}
			service.handler.onEventTime(item.wid, item.t)
		} else {
			break
		}
	}
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
