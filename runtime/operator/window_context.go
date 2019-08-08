package operator

import (
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing"
)

/*
WindowTriggerContext is used for Trigger implement TriggerContext
use factory New(windowing.WindowID) *WindowTriggerContext to create a new context
*/
type WindowTriggerContext struct {
	wid  windowing.WindowID
	wts  *WindowTimerService
	size int
}

// NewWindowTriggerContext make a context
func NewWindowTriggerContext(wts *WindowTimerService) *WindowTriggerContext {
	return &WindowTriggerContext{wts: wts}
}

// New is factory method to create new WindowTriggerContext object with param WindowID
func (c *WindowTriggerContext) New(wid windowing.WindowID, size int) *WindowTriggerContext {
	return &WindowTriggerContext{
		wid:  wid,
		wts:  c.wts,
		size: size,
	}
}

func (c *WindowTriggerContext) WindowSize() int {
	return c.size
}

func (c *WindowTriggerContext) RegisterProcessingTimer(t time.Time) {
	c.wts.RegisterProcessingTimer(c.wid, t)
}

func (c *WindowTriggerContext) RegisterEventTimer(t time.Time) {
	c.wts.RegisterEventTimer(c.wid, t)
}

func (c *WindowTriggerContext) GetCurrentEventTime() time.Time {
	return c.wts.CurrentWatermarkTime()
}

// WindowAssignerContext is context passing to window assigner
type WindowAssignerContext struct {
	wts *WindowTimerService
}

func NewWindowAssignerContext(wts *WindowTimerService) *WindowAssignerContext {
	return &WindowAssignerContext{wts: wts}
}

func (c *WindowAssignerContext) GetCurrentProcessingTime() time.Time {
	return c.wts.CurrentProcessingTime()
}
