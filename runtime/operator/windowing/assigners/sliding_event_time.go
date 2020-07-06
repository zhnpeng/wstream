package assigners

import (
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing/triggers"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
	"github.com/zhnpeng/wstream/types"
)

// SlidingEventTimeWindoww assigner
// offset represent to timezone offset duration
type SlidingEventTimeWindoww struct {
	period int64
	every  int64
	offset int64
}

func NewSlidingEventTimeWindoww(period, every, offset int64) *SlidingEventTimeWindoww {
	if offset < 0 || period <= 0 {
		panic("SlidingEventTimeWindoww params must satisfy period > 0")
	}
	return &SlidingEventTimeWindoww{
		period: period,
		every:  every,
		offset: offset,
	}
}

func (a *SlidingEventTimeWindoww) AssignWindows(item types.Item, currentTime time.Time) []windows.Window {
	var ret []windows.Window
	ts := item.Time().Unix()
	lastStart := GetWindowStartWithOffset(ts, a.offset, a.every)
	for start := lastStart; start > ts-a.period; start -= a.every {
		ret = append(ret, windows.NewTimeWindow(time.Unix(start, 0), time.Unix(start+a.period, 0)))
	}
	return ret
}

func (a *SlidingEventTimeWindoww) GetDefaultTrigger() triggers.Trigger {
	return triggers.NewEventTimeTrigger()
}

func (a *SlidingEventTimeWindoww) IsEventTime() bool {
	return true
}
