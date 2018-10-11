package operator

import (
	"math"
	"time"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/operator/windowing"
	"github.com/wandouz/wstream/runtime/operator/windowing/assigners"
	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

// Window is window operator
// assigner is due to assign windows for each record
// trigger is due to judge timing to emit window records
type Window struct {
	assigner assigners.WindowAssinger
	trigger  triggers.Trigger

	applyFunc  functions.ApplyFunc
	reduceFunc functions.ReduceFunc
	out        utils.Emitter

	windowsGroup map[windowing.WindowID]*windowing.WindowCollection

	eventTimer      *EventTimerService
	processingTimer *ProcessingTimerService
	triggerContext  *WindowTriggerContext
	assignerContext *WindowAssignerContext
}

// NewWindow make a window operator object
// default assigner is GlobalWindowAssigner
// default trigger is assigner's default trigger
// default evictor is nil
func NewWindow(assigner assigners.WindowAssinger, trigger triggers.Trigger) execution.Operator {
	if assigner == nil {
		assigner = assigners.NewGlobalWindow()
	}
	if trigger == nil {
		trigger = assigner.GetDefaultTrigger()
	}
	w := &Window{
		assigner:     assigner,
		trigger:      trigger,
		windowsGroup: make(map[windowing.WindowID]*windowing.WindowCollection),
	}
	w.processingTimer = NewProcessingTimerService(w, time.Second)
	w.eventTimer = NewEventTimerService(w)
	// bind this window to triggerContext factory
	w.triggerContext = NewWindowTriggerContext(windowing.WindowID{}, w.processingTimer, w.eventTimer)
	w.assignerContext = NewWindowAssignerContext(w.processingTimer)
	return w
}

// New is a factory method to new an Window operator object
func (w *Window) New() execution.Operator {
	return NewWindow(w.assigner, w.trigger)
}

func (w *Window) SetApplyFunc(f functions.ApplyFunc) {
	w.applyFunc = f
}

func (w *Window) SetReduceFunc(f functions.ReduceFunc) {
	w.reduceFunc = f
}

func (w *Window) handleRecord(record types.Record, out utils.Emitter) {
	assignedWindows := w.assigner.AssignWindows(record, w.assignerContext)

	for _, window := range assignedWindows {
		if w.isWindowLate(window) {
			// drop window if it is event time and late
			continue
		}
		k := utils.HashSlice(record.Key())
		wid := windowing.NewWindowID(k, window)
		var coll *windowing.WindowCollection
		if coll, ok := w.windowsGroup[wid]; ok {
			coll.PushBack(record)
		} else {
			coll = windowing.NewWindowCollection(window.MaxTimestamp(), record.Key())
			coll.PushBack(record)
			w.windowsGroup[wid] = coll
		}
		ctx := w.triggerContext.New(wid)
		signal := w.trigger.OnItem(record, record.Time(), window, ctx)
		if signal.IsFire() {
			w.emitWindow(coll, out)
		}
		if signal.IsPurge() {
			coll.Dispose()
		}
		w.registerCleanupTimer(wid, window)
	}
}

func (w *Window) emitWindow(contents *windowing.WindowCollection, out utils.Emitter) {
}

func (w *Window) registerCleanupTimer(wid windowing.WindowID, window windows.Window) {
	if window.MaxTimestamp().Equal(time.Unix(math.MaxInt64, 0)) {
		return
	}
	if w.assigner.IsEventTime() {
		w.eventTimer.RegisterEventTimer(wid, window.MaxTimestamp())
	} else {
		w.processingTimer.RegisterProcessingTimer(wid, window.MaxTimestamp())
	}
}

func (w *Window) isWindowLate(window windows.Window) bool {
	return w.assigner.IsEventTime() && window.MaxTimestamp().Before(w.eventTimer.CurrentEventTime())
}

// Window operator don't emit watermark from upstream operator
// and will emit new watermark when emit window
func (w *Window) handleWatermark(wm *types.Watermark, out utils.Emitter) {
	w.eventTimer.Drive(wm.Time())
	// out.Emit(wm)
}

// onProcessingTime is callback for processing timer service
func (w *Window) onProcessingTime(wid windowing.WindowID, t time.Time) {
	coll := w.windowsGroup[wid]
	signal := w.trigger.OnProcessingTime(t, wid.Window())
	if signal.IsFire() {
		w.emitWindow(coll, w.out)
	}
	if signal.IsPurge() {
		coll.Dispose()
	}
}

// onEventTIme is callback for event timer service
func (w *Window) onEventTime(wid windowing.WindowID, t time.Time) {
	coll := w.windowsGroup[wid]
	signal := w.trigger.OnProcessingTime(t, wid.Window())
	if signal.IsFire() {
		w.emitWindow(coll, w.out)
	}
	if signal.IsPurge() {
		coll.Dispose()
	}
}

// Run this operator
func (w *Window) Run(in *execution.Receiver, out utils.Emitter) {
	w.out = out
	consume(in, out, w)
}

// WindowTriggerContext is a factory
// implement TriggerContext and bind processing/event timer service from window operator
// use factory New(windowing.WindowID) *WindowTriggerContext to create a new context
type WindowTriggerContext struct {
	wid                    windowing.WindowID
	processingTimerService *ProcessingTimerService
	eventTimerService      *EventTimerService
}

// NewWindowTriggerContext make a context
func NewWindowTriggerContext(wid windowing.WindowID, p *ProcessingTimerService, e *EventTimerService) *WindowTriggerContext {
	return &WindowTriggerContext{
		wid: wid,
		processingTimerService: p,
		eventTimerService:      e,
	}
}

// New is factory method to create new WindowTriggerContext object with param WindowID
func (c *WindowTriggerContext) New(wid windowing.WindowID) *WindowTriggerContext {
	return &WindowTriggerContext{
		wid: wid,
		processingTimerService: c.processingTimerService,
		eventTimerService:      c.eventTimerService,
	}
}

func (c *WindowTriggerContext) RegisterProcessingTimer(t time.Time) {
	c.processingTimerService.RegisterProcessingTimer(c.wid, t)
}

func (c *WindowTriggerContext) RegisterEventTimer(t time.Time) {
	c.eventTimerService.RegisterEventTimer(c.wid, t)
}

func (c *WindowTriggerContext) GetCurrentEventTime() time.Time {
	return c.eventTimerService.CurrentEventTime()
}

type WindowAssignerContext struct {
	processingTimerService *ProcessingTimerService
}

func NewWindowAssignerContext(service *ProcessingTimerService) *WindowAssignerContext {
	return &WindowAssignerContext{
		processingTimerService: service,
	}
}

func (c *WindowAssignerContext) GetCurrentProcessingTime() time.Time {
	return c.processingTimerService.CurrentProcessingTime()
}
