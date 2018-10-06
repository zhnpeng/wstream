package triggers

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

// Trigger interface
type Trigger interface {
	OnItem(types.Item, time.Time, windows.Window, TriggerContext) TriggerSignal
	// must not register timer in OnProcessingTime
	OnProcessingTime(time.Time, windows.Window, TriggerContext) TriggerSignal
	// must not register timer in OnEventTime
	OnEventTime(time.Time, windows.Window, TriggerContext) TriggerSignal
	Dispose()
}
