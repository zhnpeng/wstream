package windows

import (
	"math"
	"sync"
	"time"
)

func NewGlobalWindow() *Window {
	return &Window{
		end: time.Unix(math.MaxInt64, 0),
	}
}

var singletonGlobalWindow *Window
var once sync.Once

// GetGlobalWindow return a singleton GlobalWindow object
func GetGlobalWindow() Window {
	once.Do(func() {
		singletonGlobalWindow = NewGlobalWindow()
	})
	return *singletonGlobalWindow
}
