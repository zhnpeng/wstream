package triggers

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type Trigger interface {
	OnItem(types.Item, time.Duration, windows.Window, *TriggerContext) TriggerSignal
	OnProcessingTime(time.Duration, windows.Window, *TriggerContext) TriggerSignal
	OnEventTime(time.Duration, windows.Window, *TriggerContext) TriggerSignal
	Dispose()
}
