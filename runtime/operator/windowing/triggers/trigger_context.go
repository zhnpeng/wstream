package triggers

import (
	"time"
)

type TriggerContext interface {
	RegisterProcessingTimer(time.Time)
	RegisterEventTimer(time.Time)
	GetCurrentEventTime() time.Time
	WindowSize() int
}
