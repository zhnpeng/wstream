package timer

import (
	"container/heap"
	"sync"
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing"
)

type ProcessingTimer struct {
	handler TimerHandler
	stoped  chan struct{}

	procHp     *TimerHeap
	procExists map[windowing.WindowID]bool
	procTicker *time.Ticker
	procCurrT  time.Time
	procMu     sync.Mutex
	procMupt   sync.RWMutex
}

func NewProcessingTimer(handler TimerHandler, d time.Duration) *ProcessingTimer {
	return &ProcessingTimer{
		handler: handler,
		stoped:  make(chan struct{}),

		procHp:     &TimerHeap{},
		procExists: make(map[windowing.WindowID]bool),
		procTicker: time.NewTicker(d),
		procCurrT:  time.Now(),
	}
}

func (timer *ProcessingTimer) OnTime(t time.Time) {
	timer.procMu.Lock()
	defer timer.procMu.Unlock()
	for timer.procHp.Len() > 0 {
		if !timer.procHp.Top().t.After(t) {
			item := heap.Pop(timer.procHp).(TimerHeapItem)
			if _, ok := timer.procExists[item.wid]; ok {
				delete(timer.procExists, item.wid)
			}
			timer.handler.OnProcessingTime(item.wid, item.t)
		} else {
			break
		}
	}
}

func (timer *ProcessingTimer) RegisterWindow(wid windowing.WindowID) {
	timer.procMu.Lock()
	defer timer.procMu.Unlock()
	if _, ok := timer.procExists[wid]; ok {
		// already registered
		return
	}
	timer.procExists[wid] = true
	heap.Push(timer.procHp, TimerHeapItem{
		t:   wid.Window().End(),
		wid: wid,
	})
}

func (timer *ProcessingTimer) CurrentTime() time.Time {
	return timer.getProcessingTime()
}

func (timer *ProcessingTimer) getProcessingTime() time.Time {
	timer.procMupt.RLock()
	defer timer.procMupt.RUnlock()
	return timer.procCurrT
}

func (timer *ProcessingTimer) setProcessingTime(t time.Time) {
	timer.procMupt.Lock()
	defer timer.procMupt.Unlock()
	timer.procCurrT = t
}

func (timer *ProcessingTimer) Start() error {
	go func() {
		defer timer.procTicker.Stop()
		for {
			select {
			case t := <-timer.procTicker.C:
				t = t.Truncate(time.Second)
				timer.setProcessingTime(t)
				timer.OnTime(t)
			case <-timer.stoped:
				timer.handler.Dispose()
				return
			}
		}
	}()
	return nil
}

func (timer *ProcessingTimer) Stop() error {
	close(timer.stoped)
	return nil
}
