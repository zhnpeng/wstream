package operator

import (
	"math"
	"time"

	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/operator/windowing/assigners"
	"github.com/wandouz/wstream/runtime/operator/windowing/basic"
	"github.com/wandouz/wstream/runtime/operator/windowing/evictors"
	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

// GroupID is window's group id
// by key and window's MaxTimestamp
type GroupID struct {
	k KeyID
	t time.Time
}

type Window struct {
	assigner assigners.WindowAssinger
	trigger  triggers.Trigger
	evictor  evictors.Evictor

	windowsGroup map[GroupID]*basic.WindowCollection

	eventTimer      *EventTimerService
	processingTimer *ProcessingTimerService
}

func NewWindow(assigner assigners.WindowAssinger, trigger triggers.Trigger, evictor evictors.Evictor) execution.Operator {
	if assigner == nil {
		assigner = assigners.NewGlobalWindow()
	}
	if trigger == nil {
		trigger = assigner.GetDefaultTrigger()
	}
	return &Window{
		assigner:        assigner,
		trigger:         trigger,
		evictor:         evictor,
		windowsGroup:    make(map[GroupID]*basic.WindowCollection),
		eventTimer:      NewEventTimerService(),
		processingTimer: NewProcessingTimerService(time.Second),
	}
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
		k := hashSlice(record.Key())
		t := window.MaxTimestamp()
		gid := GroupID{
			k: k,
			t: t,
		}
		var coll *basic.WindowCollection
		if coll, ok := w.windowsGroup[gid]; ok {
			coll.PushBack(record)
		} else {
			coll = basic.NewWindowCollection(t, record.Key())
			coll.PushBack(record)
			w.windowsGroup[gid] = coll
		}
		signal := w.trigger.OnItem(record, record.Time(), window, w)
		if signal.IsFire() {
			// TODO: emit window
			w.emitWindow(coll)
		}
		if signal.IsPurge() {
			coll.Clear()
			// TODO: clear window
		}
		w.RegisterCleanupTimer(window)
	}
}

func (w *Window) emitWindow(contents *basic.WindowCollection) {
}

func (w *Window) RegisterCleanupTimer(window windows.Window) {
	if window.MaxTimestamp().Equal(time.Unix(math.MaxInt64, 0)) {
		return
	}
	if w.assigner.IsEventTime() {
		w.eventTimer.RegisterEventTimer(window.MaxTimestamp(), w)
	} else {
		w.processingTimer.RegisterProcessingTimer(window.MaxTimestamp(), w)
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

func (w *Window) RegisterProcessingTimer(t time.Time) {
	w.processingTimer.RegisterProcessingTimer(t, w)
}

func (w *Window) RegisterEventTimer(t time.Time) {
	w.eventTimer.RegisterEventTimer(t, w)
}

func (w *Window) onProcessingTime(t time.Time) {
}

func (w *Window) onEventTime(t time.Time) {
}

func (w *Window) Run(in *execution.Receiver, out utils.Emitter) {
	consume(in, out, w)
}
