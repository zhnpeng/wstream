package windows

import (
	"time"
)

// Window does not store any items but identify a window
// using window's max timestamp, each window should
// has an unique max timestamp
type Window interface {
	// MaxTimestamp is the ID of a window
	MaxTimestamp() time.Duration
}
