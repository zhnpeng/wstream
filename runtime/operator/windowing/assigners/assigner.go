package assigners

import (
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing/triggers"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
	"github.com/zhnpeng/wstream/types"
)

type WindowAssinger interface {
	AssignWindows(types.Item, AssignerContext) []windows.Window
	GetDefaultTrigger() triggers.Trigger
	IsEventTime() bool
}

type AssignerContext interface {
	GetCurrentProcessingTime() time.Time
}
