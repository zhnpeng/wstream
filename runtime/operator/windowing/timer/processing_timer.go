package timer

import (
	"container/heap"
	"sync"
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing"
)

type ProcessingTimer struct {
	handler TimerHandler
	delay   time.Duration

	stopped    chan struct{}
	procHp     *TimerHeap
	procExists map[windowing.WindowID]bool
	procTicker *time.Ticker
	procCurrT  time.Time
	procMu     sync.Mutex
	procMupt   sync.RWMutex
}

func NewProcessingTimer(handler TimerHandler, every time.Duration, delay time.Duration) *ProcessingTimer {
	return &ProcessingTimer{
		handler: handler,
		delay:   delay,

		stopped:    make(chan struct{}),
		procHp:     &TimerHeap{},
		procExists: make(map[windowing.WindowID]bool),
		procTicker: time.NewTicker(every),
		procCurrT:  time.Now(),
	}
}

func (timer *ProcessingTimer) OnTime(t time.Time) {
	timer.procMu.Lock()
	defer timer.procMu.Unlock()
	for timer.procHp.Len() > 0 {
		// unaligned delay trigger
		if timer.procHp.Top().t.Add(timer.delay).Before(t) {
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
	// processing time use ingress time instead of window.end
	ingress := time.Now()
	heap.Push(timer.procHp, TimerHeapItem{
		t:   ingress,
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
			case <-timer.stopped:
				timer.handler.Dispose()
				return
			}
		}
	}()
	return nil
}

func (timer *ProcessingTimer) Stop() error {
	close(timer.stopped)
	return nil
}
