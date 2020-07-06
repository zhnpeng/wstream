package timer

import (
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing"
)

type Timer interface {
	// OnTime means OnProcessingTime or OnEventTime
	OnTime(t time.Time)
	// Register a window to timmer
	RegisterWindow(wid windowing.WindowID)
	// CurrentTime() return current processing time or event time
	CurrentTime() time.Time
	Start() error
	Stop() error
}
