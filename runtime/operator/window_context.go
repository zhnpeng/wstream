package operator

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing"
)

// WindowTriggerContext is a factory
// implement TriggerContext and bind processing/event timer service from window operator
// use factory New(windowing.WindowID) *WindowTriggerContext to create a new context
type WindowTriggerContext struct {
	wid  windowing.WindowID
	size int
	wts  *WindowTimerService
}

// NewWindowTriggerContext make a context
func NewWindowTriggerContext(wts *WindowTimerService) *WindowTriggerContext {
	return &WindowTriggerContext{wts: wts}
}

// New is factory method to create new WindowTriggerContext object with param WindowID
func (c *WindowTriggerContext) New(wid windowing.WindowID, size int) *WindowTriggerContext {
	return &WindowTriggerContext{
		wid:  wid,
		size: size,
		wts:  c.wts,
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
