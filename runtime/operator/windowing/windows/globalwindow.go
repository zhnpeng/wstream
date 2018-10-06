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

func (w *GlobalWindow) MaxTimestamp() time.Time {
	return time.Unix(math.MaxInt64, 0)
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
