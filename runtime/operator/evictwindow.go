package operator

import (
	"container/list"
	"math"
	"time"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/operator/windowing"
	"github.com/wandouz/wstream/runtime/operator/windowing/assigners"
	"github.com/wandouz/wstream/runtime/operator/windowing/evictors"
	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

// EvictWindow is evictable window operator
// assigner is due to assign windows for each record
// trigger is due to judge timing to emit window records
// evictor is due to evict records before or after window records are emitted
type EvictWindow struct {
	assigner assigners.WindowAssinger
	trigger  triggers.Trigger
	evictor  evictors.Evictor

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

// NewEvictWindow return evictable window object
// evictor is necessary
func NewEvictWindow(assigner assigners.WindowAssinger, trigger triggers.Trigger, evictor evictors.Evictor) utils.Operator {
	if assigner == nil {
		assigner = assigners.NewGlobalWindow()
	}
	if trigger == nil {
		trigger = assigner.GetDefaultTrigger()
	}
	if evictor == nil {
		panic("EvictWindow must has an evictor")
	}
	w := &EvictWindow{
		assigner:     assigner,
		trigger:      trigger,
		evictor:      evictor,
		windowsGroup: make(map[windowing.WindowID]*windowing.WindowCollection),
	}
	w.processingTimer = NewProcessingTimerService(w, time.Second)
	w.eventTimer = NewEventTimerService(w)
	// bind this window to triggerContext factory
	w.triggerContext = NewWindowTriggerContext(windowing.WindowID{}, w.processingTimer, w.eventTimer)
	w.assignerContext = NewWindowAssignerContext(w.processingTimer)
	return w
}

// New is a factory method to new an EvictWindow operator object
func (w *EvictWindow) New() utils.Operator {
	return NewEvictWindow(w.assigner, w.trigger, w.evictor)
}

func (w *EvictWindow) SetApplyFunc(f functions.ApplyFunc) {
	w.applyFunc = f
}

func (w *EvictWindow) SetReduceFunc(f functions.ReduceFunc) {
	w.reduceFunc = f
}

func (w *EvictWindow) handleRecord(record types.Record, out Emitter) {
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
			coll = windowing.NewWindowCollection(window, record.Time(), record.Key(), w.reduceFunc)
			coll.PushBack(record)
			w.windowsGroup[wid] = coll
		}
		ctx := w.triggerContext.New(wid)
		signal := w.trigger.OnItem(record, record.Time(), window, ctx)
		if signal.IsFire() {
			// TODO: emit window
			w.emitWindow(coll, out)
		}
		if signal.IsPurge() {
			coll.Dispose()
		}
		w.registerCleanupTimer(wid, window)
	}
}

func (w *EvictWindow) emitWindow(records *windowing.WindowCollection, out Emitter) {
	// for TimeEvictor records without timestamp is invalid
	// so for safty size should count only records with timestamp
	w.evictor.EvictBefore(records, int64(records.Len()))

	windowEmitter := NewWindowEmitter(records.Time(), out)
	iterator := records.Iterator()
	if w.reduceFunc != nil {
		acc := iterator.Value.(types.Record)
		for {
			element := iterator.Next()
			if element == nil {
				break
			}
			acc = w.reduceFunc.Reduce(acc, element.Value.(types.Record))
		}
		iter := list.New()
		iter.PushBack(acc)
		// TODO: encapsulation this
		w.applyFunc.Apply(iter.Front(), windowEmitter)
	} else {
		w.applyFunc.Apply(iterator, windowEmitter)
	}

	w.evictor.EvictAfter(records, int64(records.Len()))
}

func (w *EvictWindow) registerCleanupTimer(wid windowing.WindowID, window windows.Window) {
	if window.MaxTimestamp().Equal(time.Unix(math.MaxInt64, 0)) {
		return
	}
	if w.assigner.IsEventTime() {
		w.eventTimer.RegisterEventTimer(wid, window.MaxTimestamp())
	} else {
		w.processingTimer.RegisterProcessingTimer(wid, window.MaxTimestamp())
	}
}

func (w *EvictWindow) isWindowLate(window windows.Window) bool {
	return w.assigner.IsEventTime() && window.MaxTimestamp().Before(w.eventTimer.CurrentEventTime())
}

// EvictWindow operator don't emit watermark from upstream operator
// and will emit new watermark when emit window
func (w *EvictWindow) handleWatermark(wm *types.Watermark, out Emitter) {
	w.eventTimer.Drive(wm.Time())
	// out.Emit(wm)
}

// onProcessingTime is callback for processing timer service
func (w *EvictWindow) onProcessingTime(wid windowing.WindowID, t time.Time) {
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
func (w *EvictWindow) onEventTime(wid windowing.WindowID, t time.Time) {
	coll := w.windowsGroup[wid]
	signal := w.trigger.OnEventTime(t, wid.Window())
	if signal.IsFire() {
		w.emitWindow(coll, w.out)
	}
	if signal.IsPurge() {
		coll.Dispose()
	}
	// reemit watermark after emit windows
	w.likelyEmitWatermark()
}

// check if should emit new watermark
func (w *EvictWindow) likelyEmitWatermark() {
	eventTime := w.eventTimer.CurrentEventTime()
	if w.watermarkTime.Equal(time.Time{}) {
		w.watermarkTime = eventTime
	} else if eventTime.After(w.watermarkTime) {
		w.out.Emit(types.NewWatermark(w.watermarkTime))
		w.watermarkTime = eventTime
	}
}

// Run this operator
func (w *EvictWindow) Run(in Receiver, out Emitter) {
	w.out = out
	consume(in, out, w)
}
