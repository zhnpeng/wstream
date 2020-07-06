package timer

import (
	"container/heap"
	"sync"
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing"
)

type EventTimer struct {
	handler TimerHandler

	eventHp     *TimerHeap
	eventExists map[windowing.WindowID]bool
	eventCurrT  time.Time
	eventMu     sync.Mutex
	eventRmu    sync.RWMutex
}

func NewEventTimer(handler TimerHandler) *EventTimer {
	return &EventTimer{
		handler: handler,

		eventHp:     &TimerHeap{},
		eventExists: make(map[windowing.WindowID]bool),
	}
}

func (timer *EventTimer) OnTime(t time.Time) {
	if !t.After(timer.getEventTime()) {
		return
	}
	timer.eventMu.Lock()
	defer timer.eventMu.Unlock()
	for timer.eventHp.Len() > 0 {
		if !timer.eventHp.Top().t.After(t) {
			item := heap.Pop(timer.eventHp).(TimerHeapItem)
			// push event time forward when window's time is after current event time
			start := item.wid.Window().Start()
			if start.After(timer.getEventTime()) {
				timer.setEventTime(start)
			}
			if _, ok := timer.eventExists[item.wid]; ok {
				delete(timer.eventExists, item.wid)
			}
			timer.handler.OnEventTime(item.wid, item.t)
		} else {
			break
		}
	}
}

func (timer *EventTimer) RegisterWindow(wid windowing.WindowID) {
	timer.eventMu.Lock()
	defer timer.eventMu.Unlock()
	if _, ok := timer.eventExists[wid]; ok {
		// already registered
		return
	}
	timer.eventExists[wid] = true
	heap.Push(timer.eventHp, TimerHeapItem{
		t:   wid.Window().End(),
		wid: wid,
	})
}

func (timer *EventTimer) CurrentTime() time.Time {
	return timer.getEventTime()
}

func (timer *EventTimer) getEventTime() time.Time {
	timer.eventRmu.RLock()
	defer timer.eventRmu.RUnlock()
	return timer.eventCurrT
}

func (timer *EventTimer) setEventTime(t time.Time) {
	timer.eventRmu.Lock()
	defer timer.eventRmu.Unlock()
	timer.eventCurrT = t
}

func (timer *EventTimer) Start() error {
	// doing nothing
	return nil
}

func (timer *EventTimer) Stop() error {
	// doing nothing
	timer.handler.Dispose()
	return nil
}
