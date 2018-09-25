package windows

import "time"

type TimeWindow struct {
	start time.Duration
	end   time.Duration
}

func NewTimeWindow(start, end time.Duration) *TimeWindow {
	return &TimeWindow{start, end}
}

func (w *TimeWindow) Start() time.Duration {
	return w.start
}

func (w *TimeWindow) End() time.Duration {
	return w.end
}

func (w *TimeWindow) MaxTimestamp() time.Duration {
	return w.end - 1
}
