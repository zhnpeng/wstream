package windows

import "time"

type Window struct {
	start time.Time
	end   time.Time
}

func New(start, end time.Time) Window {
	return Window{start, end}
}

func (w Window) Start() time.Time {
	return w.start
}

func (w Window) End() time.Time {
	return w.end
}
