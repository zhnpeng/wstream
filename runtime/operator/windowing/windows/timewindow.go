package windows

import "time"

type TimeWindow struct {
	start time.Time
	end   time.Time
}

func NewTimeWindow(start, end time.Time) *TimeWindow {
	return &TimeWindow{start, end}
}

func (w *TimeWindow) Start() time.Time {
	return w.start
}

func (w *TimeWindow) End() time.Time {
	return w.end
}

func (w *TimeWindow) MaxTimestamp() time.Time {
	return w.end.Add(-1 * time.Second)
}
