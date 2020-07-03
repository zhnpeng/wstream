package operator

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"math"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/zhnpeng/wstream/functions"
	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/runtime/operator/windowing"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/assigners"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/evictors"
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
	applyFunc         functions.Apply
	reduceFunc        functions.Reduce
	out               Emitter
	windowContentsMap sync.Map // map[windowing.WindowID]*windowing.WindowContents
	watermarkTime     time.Time
	wts               *WindowTimerService
	triggerContext    *WindowTriggerContext
	assignerContext   *WindowAssignerContext
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
	w.wts = NewWindowTimerService(w, time.Second)
	// bind this window to triggerContext factory
	w.triggerContext = NewWindowTriggerContext(w.wts)
	w.assignerContext = NewWindowAssignerContext(w.wts)
	return w
}

// New is a factory method to new an EvictWindow operator object
func (w *EvictWindow) New() intfs.Operator {
	window := NewEvictWindow(w.assigner, w.trigger, w.evictor)
	window.SetApplyFunc(w.newApplyFunc())
	window.SetReduceFunc(w.newReduceFunc())
	return window
}

func (w *EvictWindow) newApplyFunc() (udf functions.Apply) {
	encodedBytes := encodeFunction(w.applyFunc)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return
}

func (w *EvictWindow) newReduceFunc() (udf functions.Reduce) {
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

func (w *EvictWindow) SetApplyFunc(f functions.Apply) {
	w.applyFunc = f
}

func (w *EvictWindow) SetReduceFunc(f functions.Reduce) {
	w.reduceFunc = f
}

func (w *EvictWindow) handleRecord(record types.Record, out Emitter) {
	assignedWindows := w.assigner.AssignWindows(record, w.assignerContext)

	key := utils.HashSlice(record.Key())
	for _, window := range assignedWindows {
		if w.isWindowLate(window) {
			// drop window if it is event time and late
			logrus.Warnf("drop late window %+v for record %+v", window, record)
			continue
		}
		wid := windowing.NewWindowID(key, window)
		var contents *windowing.WindowContents
		if c, ok := w.windowContentsMap.Load(key); ok {
			contents = c.(*windowing.WindowContents)
			contents.Append(record)
		} else {
			// evict window not reduce records in contentsections, so don't pass reduceFunc into it
			contents = windowing.NewWindowContents(window, record.Time(), record.Key(), nil)
			contents.Append(record)
			w.windowContentsMap.Store(key, contents)
		}
		ctx := w.triggerContext.New(wid, contents.Len())
		signal := w.trigger.OnItem(record, record.Time(), window, ctx)
		if signal.IsFire() {
			w.emitWindow(window, contents, out)
		}
		w.registerCleanupTimer(wid, window)
	}
}

func (w *EvictWindow) emitWindow(window windows.Window, records *windowing.WindowContents, out Emitter) {
	// for TimeEvictor records without timestamp is invalid
	// so for safty size should count only records with timestamp
	w.evictor.EvictBefore(records, int64(records.Len()))

	windowEmitter := NewWindowEmitter(window.Start(), records.Keys(), out)
	iterator := records.Iterator()
	if w.reduceFunc != nil {
		var acc types.Record
		for iter := iterator; iter != nil; iter = iter.Next() {
			if acc == nil {
				acc = w.reduceFunc.Accmulater(iter.Value.(types.Record))
			} else {
				acc = w.reduceFunc.Reduce(acc, iter.Value.(types.Record))
			}
		}
		newl := list.New()
		if acc != nil {
			newl.PushBack(acc)
		}
		// TODO: encapsulation this
		w.applyFunc.Apply(newl.Front(), windowEmitter)
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
		w.wts.RegisterEventTimer(wid, window.MaxTimestamp())
	} else {
		w.wts.RegisterProcessingTimer(wid, window.MaxTimestamp())
	}
}

func (w *EvictWindow) isWindowLate(window windows.Window) bool {
	return w.assigner.IsEventTime() && window.MaxTimestamp().Before(w.wts.CurrentWatermarkTime())
}

// EvictWindow operator don't emit watermark from upstream operator
// and will emit new watermark when emit window
func (w *EvictWindow) handleWatermark(wm *types.Watermark, out Emitter) {
	w.wts.Drive(wm.Time())
}

// onProcessingTime is callback for processing timer service
func (w *EvictWindow) onProcessingTime(wid windowing.WindowID, t time.Time) {
	c, ok := w.windowContentsMap.Load(wid.Key())
	if !ok {
		return
	}
	contents := c.(*windowing.WindowContents)
	signal := w.trigger.OnProcessingTime(t, wid.Window())
	if signal.IsFire() {
		w.emitWindow(wid.Window(), contents, w.out)
		contents.Dispose()
		w.windowContentsMap.Delete(wid)
	}
}

// onEventTIme is callback for event timer service
func (w *EvictWindow) onEventTime(wid windowing.WindowID, t time.Time) {
	c, ok := w.windowContentsMap.Load(wid.Key())
	if !ok {
		return
	}
	contents := c.(*windowing.WindowContents)

	w.likelyEmitWatermark()

	signal := w.trigger.OnEventTime(t, wid.Window())
	if signal.IsFire() {
		w.emitWindow(wid.Window(), contents, w.out)
		// clean window
		contents.Dispose()
		w.windowContentsMap.Delete(wid)
	}
}

func (w *EvictWindow) isCleanupTime(window windows.Window, t time.Time) bool {
	return t.Equal(window.MaxTimestamp())
}

// check if should emit new watermark
func (w *EvictWindow) likelyEmitWatermark() {
	eventTime := w.wts.CurrentWatermarkTime()
	if eventTime.After(w.watermarkTime) {
		w.out.Emit(types.NewWatermark(eventTime))
		w.watermarkTime = eventTime
	}
}

// Run this operator
func (w *EvictWindow) Run(in Receiver, out Emitter) {
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

func (w *EvictWindow) Dispose() {
	w.out.Dispose()
}
