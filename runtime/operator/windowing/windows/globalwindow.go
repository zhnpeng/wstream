package windows

import (
	"math"
	"time"
)

type GlobalWindow struct {
}

func NewGlobalWindow() *GlobalWindow {
	return &GlobalWindow{}
}

func (w *GlobalWindow) MaxTimestamp() time.Duration {
	return math.MaxInt64
}
