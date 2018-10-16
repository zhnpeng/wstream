package windows

import "time"

func NewTimeWindow(start, end time.Time) Window {
	return Window{start, end}
}
