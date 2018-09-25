package windows

import (
	"time"
)

type Window interface {
	MaxTimestamp() time.Duration
}
