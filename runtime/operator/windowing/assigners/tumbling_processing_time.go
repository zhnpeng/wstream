package assigners

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

// TumblingProcessingTimeWindow assigner
// offset represent to timezone offset duration
type TumblingProcessingTimeWindow struct {
	period int64
	offset int64
}

func NewTumblingProcessingTimeWindow(period, offset int64) *TumblingProcessingTimeWindow {
	if offset < 0 || period <= 0 {
		panic("TumblingProcessingTimeWindow params must satisfy period > 0")
	}
	return &TumblingProcessingTimeWindow{
		period: period,
		offset: offset,
	}
}

func (a *TumblingProcessingTimeWindow) AssignWindows(item types.Item, ctx AssignerContext) []windows.Window {
	ts := ctx.GetCurrentProcessingTime().Unix()
	start := GetWindowStartWithOffset(ts, a.offset, a.period)
	return []windows.Window{windows.NewTimeWindow(time.Unix(start, 0), time.Unix(start+a.period, 0))}
}

func (a *TumblingProcessingTimeWindow) GetDefaultTrigger() triggers.Trigger {
	return triggers.NewProcessingTimeTrigger()
}

func (a *TumblingProcessingTimeWindow) IsEventTime() bool {
	return false
}
