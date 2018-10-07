package operator

import (
	"math"
	"time"

	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/operator/windowing"
	"github.com/wandouz/wstream/runtime/operator/windowing/assigners"
	"github.com/wandouz/wstream/runtime/operator/windowing/evictors"
	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

// Window is window operator
// assigner is due to assign windows for each record
// trigger is due to judge timing to emit window records
// evictor is due to evict records before or after window records are emitted
type Window struct {
	assigner assigners.WindowAssinger
	trigger  triggers.Trigger
	evictor  evictors.Evictor

	windowsGroup map[windowing.WindowID]*windowing.WindowCollection

	eventTimer      *EventTimerService
	processingTimer *ProcessingTimerService
	triggerContext  *WindowContext
}

// NewWindow make a window operator object
// default assigner is GlobalWindowAssigner
// default trigger is assigner's default trigger
// default evictor is nil
func NewWindow(assigner assigners.WindowAssinger, trigger triggers.Trigger, evictor evictors.Evictor) execution.Operator {
	if assigner == nil {
		assigner = assigners.NewGlobalWindow()
	}
	if trigger == nil {
		trigger = assigner.GetDefaultTrigger()
	}
	w := &Window{
		assigner:     assigner,
		trigger:      trigger,
		evictor:      evictor,
		windowsGroup: make(map[windowing.WindowID]*windowing.WindowCollection),
	}
	w.processingTimer = NewProcessingTimerService(w, time.Second)
	w.eventTimer = NewEventTimerService(w)
	// bind this window to triggerContext factory
	w.triggerContext = NewWindowContext(windowing.WindowID{}, w.processingTimer, w.eventTimer)
	return w
}

// New is a factory method to new an Window operator object
func (w *Window) New() execution.Operator {
	return NewWindow(w.assigner, w.trigger, w.evictor)
}

func (w *Window) handleRecord(record types.Record, out utils.Emitter) {
	assignedWindows := w.assigner.AssignWindows(record)

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
			// TODO: emit window
			w.emitWindow(coll)
		}
		if signal.IsPurge() {
			coll.Clear()
			// TODO: clear window
		}
		w.registerCleanupTimer(wid, window)
	}
}

func (w *Window) emitWindow(contents *windowing.WindowCollection) {
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

func (w *Window) handleWatermark(wm *types.Watermark, out utils.Emitter) {

	out.Emit(wm)
}

// onProcessingTime is callback for processing timer service
func (w *Window) onProcessingTime(wid windowing.WindowID, t time.Time) {
	// signal := w.trigger.OnProcessingTime(t)
	// if signale.IsFire() {
	// 	w.emitWindow()
	// }
}

// onEventTIme is callback for event timer service
func (w *Window) onEventTime(wid windowing.WindowID, t time.Time) {
}

// Run this operator
func (w *Window) Run(in *execution.Receiver, out utils.Emitter) {
	consume(in, out, w)
}

// WindowContext is a factory
// implement TriggerContext and bind processing/event timer service from window operator
// use factory New(windowing.WindowID) *WindowContext to create a new context
type WindowContext struct {
	wid                    windowing.WindowID
	processingTimerService *ProcessingTimerService
	eventTimerService      *EventTimerService
}

// NewWindowContext make a context
func NewWindowContext(wid windowing.WindowID, p *ProcessingTimerService, e *EventTimerService) *WindowContext {
	return &WindowContext{
		wid: wid,
		processingTimerService: p,
		eventTimerService:      e,
	}
}

// New is factory method to create new WindowContext object with param WindowID
func (c *WindowContext) New(wid windowing.WindowID) *WindowContext {
	return &WindowContext{
		wid: wid,
		processingTimerService: c.processingTimerService,
		eventTimerService:      c.eventTimerService,
	}
}

func (c *WindowContext) RegisterProcessingTimer(t time.Time) {
	c.processingTimerService.RegisterProcessingTimer(c.wid, t)
}

func (c *WindowContext) RegisterEventTimer(t time.Time) {
	c.eventTimerService.RegisterEventTimer(c.wid, t)
}

func (c *WindowContext) GetCurrentEventTime() time.Time {
	return c.eventTimerService.CurrentEventTime()
}
