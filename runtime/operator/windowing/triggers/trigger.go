package triggers

import (
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
	"github.com/zhnpeng/wstream/types"
)

// Trigger interface
type Trigger interface {
	OnItem(types.Item, time.Time, windows.Window, TriggerContext) TriggerSignal
	// OnProcessingTime must not register timer in OnProcessingTime
	OnProcessingTime(time.Time, windows.Window) TriggerSignal
	// OnEventTime must not register timer in OnEventTime
	OnEventTime(time.Time, windows.Window) TriggerSignal
}
