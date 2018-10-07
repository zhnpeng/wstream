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

type Window struct {
	assigner assigners.WindowAssinger
	trigger  triggers.Trigger
	evictor  evictors.Evictor

	windowsGroup map[windowing.WindowID]*windowing.WindowCollection

	eventTimer      *EventTimerService
	processingTimer *ProcessingTimerService
	triggerContext  *WindowContext
}

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
	w.triggerContext = NewWindowContext(windowing.WindowID{}, w.processingTimer, w.eventTimer)
	return w
}

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
		w.RegisterCleanupTimer(wid, window)
	}
}

func (w *Window) emitWindow(contents *windowing.WindowCollection) {
}

func (w *Window) RegisterCleanupTimer(wid windowing.WindowID, window windows.Window) {
	if window.MaxTimestamp().Equal(time.Unix(math.MaxInt64, 0)) {
		return
	}
	if w.assigner.IsEventTime() {
		w.eventTimer.RegisterEventTimer(wid, window.MaxTimestamp())
	} else {
		w.processingTimer.RegisterProcessingTimer(wid, window.MaxTimestamp())
	}
}

func (w *Window) GetCurrentEventTime() time.Time {
	return w.eventTimer.CurrentEventTime()
}

func (w *Window) isWindowLate(window windows.Window) bool {
	return w.assigner.IsEventTime() && window.MaxTimestamp().Before(w.eventTimer.CurrentEventTime())
}

func (w *Window) handleWatermark(wm *types.Watermark, out utils.Emitter) {

	out.Emit(wm)
}

func (w *Window) RegisterProcessingTimer(wid windowing.WindowID, t time.Time) {
	w.processingTimer.RegisterProcessingTimer(wid, t)
}

func (w *Window) RegisterEventTimer(wid windowing.WindowID, t time.Time) {
	w.eventTimer.RegisterEventTimer(wid, t)
}

func (w *Window) onProcessingTime(wid windowing.WindowID, t time.Time) {
	// signal := w.trigger.OnProcessingTime(t)
	// if signale.IsFire() {
	// 	w.emitWindow()
	// }
}

func (w *Window) onEventTime(wid windowing.WindowID, t time.Time) {
}

func (w *Window) Run(in *execution.Receiver, out utils.Emitter) {
	consume(in, out, w)
}

type WindowContext struct {
	wid                    windowing.WindowID
	processingTimerService *ProcessingTimerService
	eventTimerService      *EventTimerService
}

func NewWindowContext(wid windowing.WindowID, p *ProcessingTimerService, e *EventTimerService) *WindowContext {
	return &WindowContext{
		wid: wid,
		processingTimerService: p,
		eventTimerService:      e,
	}
}

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
