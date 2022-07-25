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
	"github.com/zhnpeng/wstream/runtime/operator/windowing/evictors"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/timer"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/triggers"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
	"github.com/zhnpeng/wstream/types"
	"github.com/zhnpeng/wstream/utils"
)

// EvictWindow is evictable window operator
// assigner is due to assign windows for each record
// trigger is due to judge timing to emit window records
// evictor is due to evict records before or after window records are emitted
type EvictWindow struct {
	assigner          assigners.WindowAssinger
	trigger           triggers.Trigger
	evictor           evictors.Evictor
	applyFunc         funcintfs.Apply
	reduceFunc        funcintfs.WindowReduce
	out               Emitter
	windowContentsMap sync.Map // map[windowing.WindowID]*windowing.WindowContents
	watermarkTime     time.Time
	timer             timer.Timer
	triggerContext    *triggers.WindowTriggerContext
}

// NewEvictWindow return evictable window object
// evictor is necessary
func NewEvictWindow(assigner assigners.WindowAssinger, trigger triggers.Trigger, evictor evictors.Evictor) *EvictWindow {
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
		assigner:          assigner,
		trigger:           trigger,
		evictor:           evictor,
		windowContentsMap: sync.Map{},

		applyFunc: &byPassApplyFunc{},
	}
	if w.assigner.IsEventTime() {
		w.timer = timer.NewEventTimer(w)
	} else {
		// Count Window use a processing timer too
		// TODO: every and delay params need to be configurable, and delay in fact should pass into a trigger instead of a timer
		w.timer = timer.NewProcessingTimer(w, time.Second, 10*time.Second)
	}
	// TriggerContext factory
	w.triggerContext = triggers.NewWindowTriggerContext()
	return w
}

// New is a factory method to new an EvictWindow operator object
func (w *EvictWindow) New() intfs.Operator {
	window := NewEvictWindow(w.assigner, w.trigger, w.evictor)
	window.SetApplyFunc(w.newApplyFunc())
	window.SetReduceFunc(w.newReduceFunc())
	return window
}

func (w *EvictWindow) newApplyFunc() (udf funcintfs.Apply) {
	encodedBytes := encodeFunction(w.applyFunc)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return
}

func (w *EvictWindow) newReduceFunc() (udf funcintfs.WindowReduce) {
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

func (w *EvictWindow) SetApplyFunc(f funcintfs.Apply) {
	w.applyFunc = f
}

func (w *EvictWindow) SetReduceFunc(f funcintfs.WindowReduce) {
	w.reduceFunc = f
}

func (w *EvictWindow) handleRecord(record types.Record, out Emitter) {
	assignedWindows := w.assigner.AssignWindows(record, w.timer.CurrentTime())

	k := utils.HashSlice(record.Key())
	for _, window := range assignedWindows {
		if w.isWindowLate(window) {
			// drop window if it is event time and late
			logrus.Warnf("drop late window %+v for record %+v", window, record)
			continue
		}
		wid := windowing.NewWindowID(k, window)
		var contents *windowing.WindowContents
		if c, ok := w.windowContentsMap.Load(wid); ok {
			contents = c.(*windowing.WindowContents)
			contents.Append(record)
		} else {
			// evict window not reduce records in contentsections, so don't pass reduceFunc into it
			contents = windowing.NewWindowContents(window, record.Time(), record.Key(), nil)
			contents.Append(record)
			w.windowContentsMap.Store(wid, contents)
		}
		ctx := w.triggerContext.New(wid, contents.Len())
		currentTime := w.timer.CurrentTime()
		signal := w.trigger.OnItem(record, currentTime, window, ctx)
		if signal.IsFire() {
			w.emitWindow(contents, out)
		}
		w.registerWindow(wid)
	}
}

func (w *EvictWindow) emitWindow(content *windowing.WindowContents, out Emitter) {
	// for TimeEvictor records without timestamp is invalid
	// so for safty size should count only records with timestamp
	w.evictor.EvictBefore(content, int64(content.Len()))

	iterator := content.Iterator()
	if w.reduceFunc != nil {
		var acc types.Record
		for iter := iterator; iter != nil; iter = iter.Next() {
			if acc == nil {
				acc = w.reduceFunc.Accumulator(content.Window(), iter.Value.(types.Record))
			} else {
				acc = w.reduceFunc.Reduce(acc, iter.Value.(types.Record))
			}
		}
		newl := list.New()
		if acc != nil {
			newl.PushBack(acc)
		}
		// TODO: encapsulation this
		w.applyFunc.Apply(content.Window(), newl.Front(), out)
	} else {
		w.applyFunc.Apply(content.Window(), iterator, out)
	}

	w.evictor.EvictAfter(content, int64(content.Len()))
}

func (w *EvictWindow) registerWindow(wid windowing.WindowID) {
	if wid.Window().End().Equal(time.Unix(math.MaxInt64, 0)) {
		return
	}
	w.timer.RegisterWindow(wid)
}

func (w *EvictWindow) isWindowLate(window windows.Window) bool {
	return w.assigner.IsEventTime() && window.End().Before(w.timer.CurrentTime())
}

// EvictWindow operator don't emit watermark from upstream operator
// and will emit new watermark when emit window
func (w *EvictWindow) handleWatermark(wm *types.Watermark, out Emitter) {
	if w.assigner.IsEventTime() {
		w.timer.OnTime(wm.Time())
	}
}

// OnProcessingTime is callback for processing timer service
func (w *EvictWindow) OnProcessingTime(wid windowing.WindowID, t time.Time) {
	c, ok := w.windowContentsMap.Load(wid)
	if !ok {
		return
	}
	contents := c.(*windowing.WindowContents)
	signal := w.trigger.OnProcessingTime(t, wid.Window())
	if signal.IsFire() {
		w.emitWindow(contents, w.out)
		contents.Dispose()
		w.windowContentsMap.Delete(wid)
	}
}

// OnEventTime is callback for event timer service
func (w *EvictWindow) OnEventTime(wid windowing.WindowID, t time.Time) {
	c, ok := w.windowContentsMap.Load(wid)
	if !ok {
		return
	}
	contents := c.(*windowing.WindowContents)

	w.likelyEmitWatermark()

	signal := w.trigger.OnEventTime(t, wid.Window())
	if signal.IsFire() {
		w.emitWindow(contents, w.out)
		// clean window
		contents.Dispose()
		w.windowContentsMap.Delete(wid)
	}
}

func (w *EvictWindow) isCleanupTime(window windows.Window, t time.Time) bool {
	// return t.Equal(window.MaxTimestamp())
	return t.Equal(window.End())
}

// check if should emit new watermark
func (w *EvictWindow) likelyEmitWatermark() {
	eventTime := w.timer.CurrentTime()
	if eventTime.After(w.watermarkTime) {
		w.out.Emit(types.NewWatermark(eventTime))
		w.watermarkTime = eventTime
	}
}

// Run this operator
func (w *EvictWindow) Run(in Receiver, out Emitter) {
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

func (w *EvictWindow) Dispose() {
	w.out.Dispose()
}
