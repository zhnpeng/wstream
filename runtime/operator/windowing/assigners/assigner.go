package assigners

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type WindowAssinger interface {
	AssignWindows(types.Item, AssignerContext) []windows.Window
	GetDefaultTrigger() triggers.Trigger
	IsEventTime() bool
}

type AssignerContext interface {
	GetCurrentProcessingTime() time.Time
}
