package windows

import (
	"math"
	"sync"
	"time"
)

type GlobalWindow struct {
}

func NewGlobalWindow() *GlobalWindow {
	return &GlobalWindow{}
}

// MaxTimestamp is the ID of a window
func (w *GlobalWindow) MaxTimestamp() time.Duration {
	return math.MaxInt64
}

var singletonGlobalWindow *GlobalWindow
var once sync.Once

// GetGlobalWindow return a singleton GlobalWindow object
func GetGlobalWindow() *GlobalWindow {
	once.Do(func() {
		singletonGlobalWindow = NewGlobalWindow()
	})
	return singletonGlobalWindow
}
