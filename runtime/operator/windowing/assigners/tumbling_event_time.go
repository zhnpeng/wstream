package assigners

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

// TumblingEventTimeWindow assigner
// offset represent to timezone offset duration
type TumblingEventTimeWindow struct {
	period int64
	offset int64
}

func NewTumblingEventTimeWindow(period, offset int64) *TumblingEventTimeWindow {
	if offset < 0 || period <= 0 {
		panic("TumblingEventTimeWindow params must satisfy period > 0")
	}
	return &TumblingEventTimeWindow{
		period: period,
		offset: offset,
	}
}

func (a *TumblingEventTimeWindow) AssignWindows(item types.Item, ctx AssignerContext) []windows.Window {
	ts := item.Time().Unix()
	start := GetWindowStartWithOffset(ts, a.offset, a.period)
	return []windows.Window{windows.NewTimeWindow(time.Unix(start, 0), time.Unix(start+a.period, 0))}
}

func (a *TumblingEventTimeWindow) GetDefaultTrigger() triggers.Trigger {
	return triggers.NewEventTimeTrigger()
}

func (a *TumblingEventTimeWindow) IsEventTime() bool {
	return true
}
