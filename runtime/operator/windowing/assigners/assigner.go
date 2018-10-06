package assigners

import (
	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type WindowAssinger interface {
	AssignWindows(types.Item) []windows.Window
	GetDefaultTrigger() triggers.Trigger
	IsEventTime() bool
}
