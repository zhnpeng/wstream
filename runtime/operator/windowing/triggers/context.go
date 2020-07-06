package triggers

import (
	"github.com/zhnpeng/wstream/runtime/operator/windowing"
)

/*
WindowTriggerContext is used for Trigger implement TriggerContext
use factory New(windowing.WindowID) *WindowTriggerContext to create a new context
*/
type WindowTriggerContext struct {
	wid  windowing.WindowID
	size int
}

// NewWindowTriggerContext make a context
func NewWindowTriggerContext() *WindowTriggerContext {
	return &WindowTriggerContext{}
}

// New is factory method to create new WindowTriggerContext object with param WindowID
func (c *WindowTriggerContext) New(wid windowing.WindowID, size int) *WindowTriggerContext {
	return &WindowTriggerContext{
		wid:  wid,
		size: size,
	}
}

func (c *WindowTriggerContext) WindowSize() int {
	return c.size
}
