package operator

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"math"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/zhnpeng/wstream/funcintfs"
	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/runtime/operator/windowing"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/assigners"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/timer"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/triggers"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
	"github.com/zhnpeng/wstream/types"
	"github.com/zhnpeng/wstream/utils"
)

type WindowOperator interface {
	intfs.Operator
	SetReduceFunc(funcintfs.WindowReduce)
	SetApplyFunc(funcintfs.Apply)
}

type byPassApplyFunc struct{}

func (*byPassApplyFunc) Apply(window windows.Window, records *list.Element, out funcintfs.Emitter) {
	for elem := records; elem != nil; elem = elem.Next() {
		out.Emit(elem.Value.(types.Item))
	}
}

// Window is window operator
// assigner is due to assign windows for each record
// trigger is due to judge timing to emit window records
type Window struct {
	assigner          assigners.WindowAssinger
	trigger           triggers.Trigger
	applyFunc         funcintfs.Apply
	reduceFunc        funcintfs.WindowReduce
	out               Emitter
	windowContentsMap sync.Map // map[windowing.WindowID]*windowing.WindowContents
	watermarkTime     time.Time
	timer             timer.Timer
	triggerContext    *triggers.WindowTriggerContext
}

/*
NewWindow make a window operator object

	PARAMS
		default assigner is GlobalWindowAssigner
		default trigger is assigner's default trigger
		default evictor is nil

	DESCRIPTION:

		About watermark
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
		assigner:          assigner,
		trigger:           trigger,
		windowContentsMap: sync.Map{},

		applyFunc: &byPassApplyFunc{},
	}
	if assigner.IsEventTime() {
		w.timer = timer.NewEventTimer(w)
	} else {
		// Count Window use a processing timer too
		w.timer = timer.NewProcessingTimer(w, time.Second)
	}
	// bind this window to triggerContext factory
	w.triggerContext = triggers.NewWindowTriggerContext()
	return w
}

// New is a factory method to new an Window operator object
func (w *Window) New() intfs.Operator {
	window := NewWindow(w.assigner, w.trigger)
	window.SetApplyFunc(w.newApplyFunc())
	window.SetReduceFunc(w.newReduceFunc())
	return window
}

func (w *Window) newApplyFunc() (udf funcintfs.Apply) {
	encodedBytes := encodeFunction(w.applyFunc)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return
}

func (w *Window) newReduceFunc() (udf funcintfs.WindowReduce) {
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

func (w *Window) SetApplyFunc(f funcintfs.Apply) {
	if f == nil {
		logrus.Warnf("Passing a nil apply function to window apply")
		return
	}
	w.applyFunc = f
}

func (w *Window) SetReduceFunc(f funcintfs.WindowReduce) {
	if f == nil {
		logrus.Warnf("Passing a nil reduce function to window reduce")
		return
	}
	w.reduceFunc = f
}

func (w *Window) handleRecord(record types.Record, out Emitter) {
	assignedWindows := w.assigner.AssignWindows(record, w.timer.CurrentTime())

	for _, window := range assignedWindows {
		if w.isWindowLate(window) {
			// drop window if it is event time and late
			logrus.Warnf("drop late window (%+v %+v) for record %+v, watermark time is %v", window.Start(), window.End(), record, w.timer.CurrentTime())
			continue
		}
		k := utils.HashSlice(record.Key())
		wid := windowing.NewWindowID(k, window)
		var contents *windowing.WindowContents
		if c, ok := w.windowContentsMap.Load(wid); ok {
			contents = c.(*windowing.WindowContents)
			contents.Append(record)
		} else {
			contents = windowing.NewWindowContents(window, record.Time(), record.Key(), w.reduceFunc)
			contents.Append(record)
			w.windowContentsMap.Store(wid, contents)
		}
		ctx := w.triggerContext.New(wid, contents.Len())
		signal := w.trigger.OnItem(record, record.Time(), window, ctx)
		if signal.IsFire() {
			w.emitWindow(window, contents, out)
		}
		w.registerWindow(wid)
	}
}

func (w *Window) emitWindow(window windows.Window, contents *windowing.WindowContents, out Emitter) {
	iterator := contents.Iterator()
	w.applyFunc.Apply(window, iterator, out)
}

func (w *Window) registerWindow(wid windowing.WindowID) {
	if wid.Window().End().Equal(time.Unix(math.MaxInt64, 0)) {
		// ignore GlobalWindow
		return
	}
	w.timer.RegisterWindow(wid)
}

func (w *Window) isWindowLate(window windows.Window) bool {
	return w.assigner.IsEventTime() && window.End().Before(w.timer.CurrentTime())
}

func (w *Window) handleWatermark(wm *types.Watermark, out Emitter) {
	//window do multi way merge watermarks into one
	//and drive event time with it
	if w.assigner.IsEventTime() {
		w.timer.OnTime(wm.Time())
	}
}

// OnProcessingTime is callback for processing timer service
func (w *Window) OnProcessingTime(wid windowing.WindowID, t time.Time) {
	c, ok := w.windowContentsMap.Load(wid)
	if !ok {
		return
	}
	contents := c.(*windowing.WindowContents)
	signal := w.trigger.OnProcessingTime(t, wid.Window())
	if signal.IsFire() {
		w.emitWindow(wid.Window(), contents, w.out)
		// dispose window content
		contents.Dispose()
		w.windowContentsMap.Delete(wid)
	}
}

// OnEventTime is callback for event timer service
func (w *Window) OnEventTime(wid windowing.WindowID, t time.Time) {
	c, ok := w.windowContentsMap.Load(wid)
	if !ok {
		return
	}
	contents := c.(*windowing.WindowContents)
	// reemit watermark before emit windows
	w.likelyEmitWatermark()

	signal := w.trigger.OnEventTime(t, wid.Window())
	if signal.IsFire() {
		w.emitWindow(wid.Window(), contents, w.out)
		// dispose window content
		contents.Dispose()
		w.windowContentsMap.Delete(wid)
	}
}

func (w *Window) isCleanupTime(window windows.Window, t time.Time) bool {
	return t.Equal(window.End())
}

// check if should emit new watermark
func (w *Window) likelyEmitWatermark() {
	eventTime := w.timer.CurrentTime()
	if eventTime.After(w.watermarkTime) {
		w.out.Emit(types.NewWatermark(eventTime))
		w.watermarkTime = eventTime
	}
}

// Run this operator
func (w *Window) Run(in Receiver, out Emitter) {
	w.out = out
	w.timer.Start()
	defer w.timer.Stop()
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
