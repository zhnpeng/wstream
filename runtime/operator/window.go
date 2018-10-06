package operator

import (
	"math"
	"time"

	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/operator/windowing/assigners"
	"github.com/wandouz/wstream/runtime/operator/windowing/basic"
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

	windowsGroup map[GroupID]*basic.WindowCollection

	eventTimer      *EventTimerService
	processingTimer *ProcessingTimerService
}

func NewWindow(assigner assigners.WindowAssinger, trigger triggers.Trigger) execution.Operator {
	if assigner == nil {
		assigner = assigners.NewGlobalWindow()
	}
	if trigger == nil {
		trigger = assigner.GetDefaultTrigger()
	}
	return &Window{
		assigner:        assigner,
		trigger:         trigger,
		windowsGroup:    make(map[GroupID]*WindowCollection),
		eventTimer:      NewEventTimerService(),
		processingTimer: NewProcessingTimerService(time.Second),
	}
}

func (w *Window) New() execution.Operator {
	return NewWindow(w.assigner, w.trigger)
}

func (w *Window) handleRecord(record types.Record, out utils.Emitter) {
	assignedWindows := w.assigner.AssignWindows(record)

	for _, window := range assignedWindows {
		if w.isWindowLate(window) {
			// drop event time late window
			continue
		}
		k := hashSlice(record.Key())
		t := window.MaxTimestamp()
		gid := GroupID{
			k: k,
			t: t,
		}
		if wrec, ok := w.windowsGroup[gid]; ok {
			wrec.PushBack(record)
		} else {
			newColl := NewWindowCollection(t, record.Key())
			newColl.PushBack(record)
			w.windowsGroup[gid] = newColl
		}
		signal := w.trigger.OnItem(record, record.Time(), window, w)
		if signal.IsFire() {
			// TODO: emit window
		}
		if signal.IsPurge() {
			// TODO: clear window
		}
		w.RegisterCleanupTimer(window)
	}
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

func (w *Window) isWindowLate(window windows.Window) bool {
	return w.assigner.IsEventTime() && window.MaxTimestamp().Before(w.eventTimer.CurrentEventTime())
}

func (w *Window) handleWatermark(wm *types.Watermark, out utils.Emitter) {

	out.Emit(wm)
}

func (w *Window) onProcessingTime(t time.Time) {

}

func (w *Window) onEventTime(t time.Time) {

}

func (w *Window) Run(in *execution.Receiver, out utils.Emitter) {
	consume(in, out, w)
}
