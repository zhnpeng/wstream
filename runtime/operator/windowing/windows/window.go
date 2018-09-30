package windows

import (
	"time"
)

// Window is interface of all kinds of windows
type Window interface {
	// MaxTimestamp is the ID of a window
	MaxTimestamp() time.Duration
}
