package timer

import (
	"container/heap"
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing"
)

type EventTimer struct {
	handler TimerHandler

	eventHp     *TimerHeap
	eventExists map[windowing.WindowID]bool
	eventCurrT  time.Time
}

func NewEventTimer(handler TimerHandler) *EventTimer {
	return &EventTimer{
		handler: handler,

		eventHp:     &TimerHeap{},
		eventExists: make(map[windowing.WindowID]bool),
	}
}

func (timer *EventTimer) OnTime(t time.Time) {
	if !t.After(timer.eventCurrT) {
		return
	}
	for timer.eventHp.Len() > 0 {
		if !timer.eventHp.Top().t.After(t) {
			item := heap.Pop(timer.eventHp).(TimerHeapItem)
			// push event time forward when window's time is after current event time
			start := item.wid.Window().Start()
			if start.After(timer.eventCurrT) {
				timer.eventCurrT = start
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
	return timer.eventCurrT
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
