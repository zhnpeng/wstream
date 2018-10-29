package operator

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"math"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/runtime/operator/windowing"
	"github.com/wandouz/wstream/runtime/operator/windowing/assigners"
	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
	"github.com/wandouz/wstream/utils"
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

	windowsGroup sync.Map
	// windowsGroup map[windowing.WindowID]*windowing.WindowCollection

	watermarkTime   time.Time
	wts             *WindowTimerService
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
func NewWindow(assigner assigners.WindowAssinger, trigger triggers.Trigger) *Window {
	if assigner == nil {
		assigner = assigners.NewGlobalWindow()
	}
	if trigger == nil {
		trigger = assigner.GetDefaultTrigger()
	}
	w := &Window{
		assigner:     assigner,
		trigger:      trigger,
		windowsGroup: sync.Map{},

		applyFunc: &byPassApplyFunc{},
	}
	w.wts = NewWindowTimerService(w, time.Second)
	// bind this window to triggerContext factory
	w.triggerContext = NewWindowTriggerContext(w.wts)
	w.assignerContext = NewWindowAssignerContext(w.wts)
	return w
}

// New is a factory method to new an Window operator object
func (w *Window) New() intfs.Operator {
	window := NewWindow(w.assigner, w.trigger)
	window.SetApplyFunc(w.newApplyFunc())
	window.SetReduceFunc(w.newReduceFunc())
	return window
}

func (w *Window) newApplyFunc() (udf functions.ApplyFunc) {
	encodedBytes := encodeFunction(w.applyFunc)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return
}

func (w *Window) newReduceFunc() (udf functions.ReduceFunc) {
	if w.reduceFunc == nil {
		return
	}
	encodedBytes := encodeFunction(w.reduceFunc)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return
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
		logrus.Warnf("Passing a nil reduce function to window reduce")
		return
	}
	w.reduceFunc = f
}

func (w *Window) handleRecord(record types.Record, out Emitter) {
	assignedWindows := w.assigner.AssignWindows(record, w.assignerContext)

	for _, window := range assignedWindows {
		if w.isWindowLate(window) {
			// drop window if it is event time and late
			logrus.Warnf("drop late window (%+v %+v) for record %+v, watermark time is %v", window.Start(), window.End(), record, w.wts.CurrentWatermarkTime())
			continue
		}
		k := utils.HashSlice(record.Key())
		wid := windowing.NewWindowID(k, window)
		var coll *windowing.WindowCollection
		if c, ok := w.windowsGroup.Load(wid); ok {
			coll = c.(*windowing.WindowCollection)
			coll.Append(record)
		} else {
			coll = windowing.NewWindowCollection(window, record.Time(), record.Key(), w.reduceFunc)
			coll.Append(record)
			w.windowsGroup.Store(wid, coll)
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
	emitter := NewWindowEmitter(records.Time(), records.Keys(), out)
	iterator := records.Iterator()
	w.applyFunc.Apply(iterator, emitter)
}

func (w *Window) registerCleanupTimer(wid windowing.WindowID, window windows.Window) {
	if window.MaxTimestamp().Equal(time.Unix(math.MaxInt64, 0)) {
		// ignore GlobalWindow
		return
	}
	if w.assigner.IsEventTime() {
		w.wts.RegisterEventTimer(wid, window.MaxTimestamp())
	} else {
		w.wts.RegisterProcessingTimer(wid, window.MaxTimestamp())
	}
}

func (w *Window) isWindowLate(window windows.Window) bool {
	return w.assigner.IsEventTime() && window.MaxTimestamp().Before(w.wts.CurrentWatermarkTime())
}

func (w *Window) handleWatermark(wm *types.Watermark, out Emitter) {
	//window do multi way merge watermarks into one
	//and drive event time with it
	w.wts.Drive(wm.Time())
}

// onProcessingTime is callback for processing timer service
func (w *Window) onProcessingTime(wid windowing.WindowID, t time.Time) {
	c, ok := w.windowsGroup.Load(wid)
	if !ok {
		return
	}
	coll := c.(*windowing.WindowCollection)
	signal := w.trigger.OnProcessingTime(t, wid.Window())
	if signal.IsFire() {
		w.emitWindow(coll, w.out)
	}
	if signal.IsPurge() {
		coll.Dispose()
	}
	if !w.assigner.IsEventTime() && w.isCleanupTime(wid.Window(), t) {
		coll.Dispose()
		w.windowsGroup.Delete(wid)
	}
}

// onEventTIme is callback for event timer service
func (w *Window) onEventTime(wid windowing.WindowID, t time.Time) {
	c, ok := w.windowsGroup.Load(wid)
	if !ok {
		return
	}
	coll := c.(*windowing.WindowCollection)
	// reemit watermark before emit windows
	w.likelyEmitWatermark()

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
		w.windowsGroup.Delete(wid)
	}
}

func (w *Window) isCleanupTime(window windows.Window, t time.Time) bool {
	return t.Equal(window.MaxTimestamp())
}

// check if should emit new watermark
func (w *Window) likelyEmitWatermark() {
	eventTime := w.wts.CurrentWatermarkTime()
	if eventTime.After(w.watermarkTime) {
		w.out.Emit(types.NewWatermark(eventTime))
		w.watermarkTime = eventTime
	}
}

// Run this operator
func (w *Window) Run(in Receiver, out Emitter) {
	w.out = out
	w.wts.Start()
	defer w.wts.Stop()
	for {
		item, ok := <-in.Next()
		if !ok {
			return
		}
		switch item.(type) {
		case types.Record:
			w.handleRecord(item.(types.Record), out)
		case *types.Watermark:
			w.handleWatermark(item.(*types.Watermark), out)
		}
	}
}

func (w *Window) Dispose() {
	w.out.Dispose()
}
