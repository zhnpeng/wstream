package operator

import (
	"container/list"
	"math"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/operator/windowing"
	"github.com/wandouz/wstream/runtime/operator/windowing/assigners"
	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

type byPassApplyFunc struct{}

func (*byPassApplyFunc) Apply(records *list.Element, out functions.Emitter) {
	for elem := records; elem != nil; elem = elem.Next() {
		out.Emit(elem.Value.(types.Item))
	}
}

// Window is window operator
// assigner is due to assign windows for each record
// trigger is due to judge timing to emit window records
type Window struct {
	assigner assigners.WindowAssinger
	trigger  triggers.Trigger

	applyFunc  functions.ApplyFunc
	reduceFunc functions.ReduceFunc
	out        Emitter

	windowsGroup map[windowing.WindowID]*windowing.WindowCollection

	watermarkTime   time.Time
	eventTimer      *EventTimerService
	processingTimer *ProcessingTimerService
	triggerContext  *WindowTriggerContext
	assignerContext *WindowAssignerContext
}

/*
NewWindow make a window operator object
params:
	default assigner is GlobalWindowAssigner
	default trigger is assigner's default trigger
	default evictor is nil
Some notices
1. about watermark
only when time charaacteristic is event time watermark make sences
window will swallow all watermarks from upstream operator
and regenerate new watermark to downstream according to window's fire time
count window won't generate any watermark
*/
func NewWindow(assigner assigners.WindowAssinger, trigger triggers.Trigger) utils.Operator {
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

		applyFunc: &byPassApplyFunc{},
	}
	w.processingTimer = NewProcessingTimerService(w, time.Second)
	w.eventTimer = NewEventTimerService(w)
	// bind this window to triggerContext factory
	w.triggerContext = NewWindowTriggerContext(w.processingTimer, w.eventTimer)
	w.assignerContext = NewWindowAssignerContext(w.processingTimer)
	return w
}

// New is a factory method to new an Window operator object
func (w *Window) New() utils.Operator {
	return NewWindow(w.assigner, w.trigger)
}

func (w *Window) SetApplyFunc(f functions.ApplyFunc) {
	if f == nil {
		logrus.Warnf("Passing a nil apply function to window apply")
		return
	}
	w.applyFunc = f
}

func (w *Window) SetReduceFunc(f functions.ReduceFunc) {
	if f == nil {
		logrus.Warnf("Passing a nil reduce function to window apply")
		return
	}
	w.reduceFunc = f
}

func (w *Window) handleRecord(record types.Record, out Emitter) {
	assignedWindows := w.assigner.AssignWindows(record, w.assignerContext)

	for _, window := range assignedWindows {
		if w.isWindowLate(window) {
			// drop window if it is event time and late
			logrus.Warnf("drop late window %+v for record %+v", window, record)
			continue
		}
		k := utils.HashSlice(record.Key())
		wid := windowing.NewWindowID(k, window)
		var coll *windowing.WindowCollection
		if c, ok := w.windowsGroup[wid]; ok {
			coll.Append(record)
			coll = c
		} else {
			coll = windowing.NewWindowCollection(window, record.Time(), record.Key(), w.reduceFunc)
			coll.Append(record)
			w.windowsGroup[wid] = coll
		}
		ctx := w.triggerContext.New(wid, coll.Len())
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

func (w *Window) emitWindow(records *windowing.WindowCollection, out Emitter) {
	emitter := NewWindowEmitter(records.Time(), out)
	iterator := records.Iterator()
	w.applyFunc.Apply(iterator, emitter)
}

func (w *Window) registerCleanupTimer(wid windowing.WindowID, window windows.Window) {
	if window.MaxTimestamp().Equal(time.Unix(math.MaxInt64, 0)) {
		// ignore GlobalWindow
		return
	}
	if w.assigner.IsEventTime() {
		w.eventTimer.RegisterEventTimer(wid, window.MaxTimestamp())
	} else {
		w.processingTimer.RegisterProcessingTimer(wid, window.MaxTimestamp())
	}
}

func (w *Window) isWindowLate(window windows.Window) bool {
	return w.assigner.IsEventTime() && window.MaxTimestamp().Before(w.eventTimer.CurrentWatermarkTime())
}

func (w *Window) handleWatermark(wm *types.Watermark, out Emitter) {
	// EventTimerService emit watermark
	// so Window Operator with CountAssigner won't
	// emit watermark to down stream operator
	w.eventTimer.Drive(wm.Time())
}

// onProcessingTime is callback for processing timer service
func (w *Window) onProcessingTime(wid windowing.WindowID, t time.Time) {
	coll, ok := w.windowsGroup[wid]
	if !ok {
		return
	}
	signal := w.trigger.OnProcessingTime(t, wid.Window())
	if signal.IsFire() {
		w.emitWindow(coll, w.out)
	}
	if signal.IsPurge() {
		coll.Dispose()
	}
	if !w.assigner.IsEventTime() && w.isCleanupTime(wid.Window(), t) {
		coll.Dispose()
		delete(w.windowsGroup, wid)
	}
}

// onEventTIme is callback for event timer service
func (w *Window) onEventTime(wid windowing.WindowID, t time.Time) {
	coll, ok := w.windowsGroup[wid]
	if !ok {
		return
	}
	signal := w.trigger.OnEventTime(t, wid.Window())
	if signal.IsFire() {
		w.emitWindow(coll, w.out)
	}
	if signal.IsPurge() {
		coll.Dispose()
	}
	if w.assigner.IsEventTime() && w.isCleanupTime(wid.Window(), t) {
		// clean window
		coll.Dispose()
		delete(w.windowsGroup, wid)
	}
	// reemit watermark after emit windows
	w.likelyEmitWatermark()
}

func (w *Window) isCleanupTime(window windows.Window, t time.Time) bool {
	return t.Equal(window.MaxTimestamp())
}

// check if should emit new watermark
func (w *Window) likelyEmitWatermark() {
	eventTime := w.eventTimer.CurrentWatermarkTime()
	if w.watermarkTime.Equal(time.Time{}) {
		w.watermarkTime = eventTime
	} else if eventTime.After(w.watermarkTime) {
		w.out.Emit(types.NewWatermark(w.watermarkTime))
		w.watermarkTime = eventTime
	}
}

// Run this operator
func (w *Window) Run(in Receiver, out Emitter) {
	// FIXME: emitter may be property of operator
	w.out = out
	consume(in, out, w)
}

// WindowTriggerContext is a factory
// implement TriggerContext and bind processing/event timer service from window operator
// use factory New(windowing.WindowID) *WindowTriggerContext to create a new context
type WindowTriggerContext struct {
	wid  windowing.WindowID
	size int
	ets  *EventTimerService
	pts  *ProcessingTimerService
}

// NewWindowTriggerContext make a context
func NewWindowTriggerContext(p *ProcessingTimerService, e *EventTimerService) *WindowTriggerContext {
	return &WindowTriggerContext{
		pts: p,
		ets: e,
	}
}

// New is factory method to create new WindowTriggerContext object with param WindowID
func (c *WindowTriggerContext) New(wid windowing.WindowID, size int) *WindowTriggerContext {
	return &WindowTriggerContext{
		wid:  wid,
		size: size,
		pts:  c.pts,
		ets:  c.ets,
	}
}

func (c *WindowTriggerContext) WindowSize() int {
	return c.size
}

func (c *WindowTriggerContext) RegisterProcessingTimer(t time.Time) {
	c.pts.RegisterProcessingTimer(c.wid, t)
}

func (c *WindowTriggerContext) RegisterEventTimer(t time.Time) {
	c.ets.RegisterEventTimer(c.wid, t)
}

func (c *WindowTriggerContext) GetCurrentEventTime() time.Time {
	return c.ets.CurrentWatermarkTime()
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
