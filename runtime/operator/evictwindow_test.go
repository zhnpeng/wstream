package operator

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing/evictors"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
	"github.com/zhnpeng/wstream/utils"

	"github.com/google/go-cmp/cmp"

	"github.com/spf13/cast"
	"github.com/zhnpeng/wstream/env"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/assigners"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/triggers"
	"github.com/zhnpeng/wstream/types"
)

type evictWindowTestReceiver struct {
	ch chan types.Item
}

func (r *evictWindowTestReceiver) Next() <-chan types.Item {
	return r.ch
}

type evictWindowTestEmitter struct {
	ch chan types.Item
}

func (e *evictWindowTestEmitter) Length() int {
	return 1
}

func (e *evictWindowTestEmitter) Emit(item types.Item) {
	e.ch <- item
}

func (e *evictWindowTestEmitter) EmitTo(index int, item types.Item) error {
	return nil
}

func (e *evictWindowTestEmitter) Dispose() {
	close(e.ch)
}

type evictWindowTestReduceFunc struct {
}

func (f *evictWindowTestReduceFunc) Accmulater(window windows.Window, x types.Record) types.Record {
	var t time.Time
	if window.Start().Equal(t) {
		// for count window window's time is the first record's time
		t = x.Time()
	} else {
		t = window.Start()
	}
	ret := map[string]interface{}{
		"A": cast.ToInt(x.Get("A")),
		"B": cast.ToInt(x.Get("B")),
	}
	return types.NewMapRecord(t, ret)
}

func (f *evictWindowTestReduceFunc) Reduce(x types.Record, y types.Record) types.Record {
	x.Set("A", int(math.Max(cast.ToFloat64(x.Get("A")), cast.ToFloat64(y.Get("A")))))
	x.Set("B", cast.ToInt(x.Get("B"))+cast.ToInt(y.Get("B")))
	return x
}

func TestEvictWindow_Run_Tumbling_EventTime_Window(t *testing.T) {
	items := []types.Item{
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:00:00"),
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:00:00"),
			map[string]interface{}{
				"A": 1,
				"B": 1,
			},
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:00:01"),
			map[string]interface{}{
				"A": 2,
				"B": 2,
			},
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:00:59"),
			map[string]interface{}{
				"A": 3,
				"B": 3,
			},
		),
		types.NewMapRecord(
			utils.ParseTimeMilli("2018-10-15 18:00:59.999"),
			map[string]interface{}{
				"A": 4,
				"B": 4,
			},
		),
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:01:00"),
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:01:00"),
			map[string]interface{}{
				"A": 4,
				"B": 4,
			},
		),
		types.NewMapRecord(
			utils.ParseTimeMilli("2018-10-15 18:01:59.999"),
			map[string]interface{}{
				"A": 5,
				"B": 5,
			},
		),
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:02:00"),
		),
	}
	input := make(chan types.Item)
	output := make(chan types.Item)

	receiver := &evictWindowTestReceiver{
		ch: input,
	}

	emitter := &evictWindowTestEmitter{
		ch: output,
	}

	env.Env().TimeCharacteristic = env.IsEventTime
	assigner := assigners.NewTumblingEventTimeWindow(60, 0)
	trigger := triggers.NewEventTimeTrigger()
	// time evictor evict records
	evictor := evictors.NewTimeEvictor().Of(1000, false) // 1000 represent to 1000 milliseconds
	w := NewEvictWindow(assigner, trigger, evictor)
	w.SetReduceFunc(&evictWindowTestReduceFunc{})

	var wg sync.WaitGroup

	go func() {
		for _, record := range items {
			input <- record
		}
		close(input)
	}()

	var got []types.Item
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			i, ok := <-output
			if !ok {
				break
			}
			got = append(got, i)
		}
	}()

	// wait until all item is emit to output
	w.Run(receiver, emitter)
	wg.Wait()

	want := []types.Item{
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:00:00"),
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:00:00"),
			map[string]interface{}{
				"A": 4,
				"B": 7,
			},
		),
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:01:00"),
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:01:00"),
			map[string]interface{}{
				"A": 5,
				"B": 5,
			},
		),
	}

	if diff := cmp.Diff(got, want); diff != "" {
		t.Error(diff)
	}
}

func TestEvictWindow_Run_Tumbling_Count_Window(t *testing.T) {
	items := []types.Item{
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:00:00"),
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:00:00"),
			map[string]interface{}{
				"A": 1,
				"B": 1,
			},
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:00:01"),
			map[string]interface{}{
				"A": 2,
				"B": 2,
			},
		),
		types.NewMapRecord(
			utils.ParseTimeMilli("2018-10-15 18:00:59.999"),
			map[string]interface{}{
				"A": 3,
				"B": 3,
			},
		),
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:01:00"),
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:01:00"),
			map[string]interface{}{
				"A": 4,
				"B": 4,
			},
		),
		types.NewMapRecord(
			utils.ParseTimeMilli("2018-10-15 18:01:59.999"),
			map[string]interface{}{
				"A": 5,
				"B": 5,
			},
		),
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:02:00"),
		),
	}
	input := make(chan types.Item)
	output := make(chan types.Item)

	receiver := &evictWindowTestReceiver{
		ch: input,
	}

	emitter := &evictWindowTestEmitter{
		ch: output,
	}

	env.Env().TimeCharacteristic = env.IsEventTime
	assigner := assigners.NewGlobalWindow()
	trigger := triggers.NewCountTrigger().Of(2)
	evictor := evictors.NewCountEvictor().Of(2, true)
	w := NewEvictWindow(assigner, trigger, evictor)
	w.SetReduceFunc(&evictWindowTestReduceFunc{})

	var wg sync.WaitGroup

	go func() {
		for _, record := range items {
			input <- record
		}
		close(input)
	}()

	var got []types.Item
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			i, ok := <-output
			if !ok {
				break
			}
			got = append(got, i)
		}
	}()

	// wait until all item is emit to output
	w.Run(receiver, emitter)
	wg.Wait()

	want := []types.Item{
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:00:00"),
			map[string]interface{}{
				"A": 2,
				"B": 3,
			},
		),
		types.NewMapRecord(
			utils.ParseTimeMilli("2018-10-15 18:00:59.999"),
			map[string]interface{}{
				"A": 4,
				"B": 7,
			},
		),
	}

	if diff := cmp.Diff(got, want); diff != "" {
		t.Error(diff)
	}
}

func TestEvictWindow_Run_Sliding_Count_Window(t *testing.T) {
	items := []types.Item{
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:00:00"),
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:00:00"),
			map[string]interface{}{
				"A": 1,
				"B": 1,
			},
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:00:01"),
			map[string]interface{}{
				"A": 2,
				"B": 2,
			},
		),
		types.NewMapRecord(
			utils.ParseTimeMilli("2018-10-15 18:00:59.999"),
			map[string]interface{}{
				"A": 3,
				"B": 3,
			},
		),
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:01:00"),
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:01:00"),
			map[string]interface{}{
				"A": 4,
				"B": 4,
			},
		),
		types.NewMapRecord(
			utils.ParseTimeMilli("2018-10-15 18:01:59.999"),
			map[string]interface{}{
				"A": 5,
				"B": 5,
			},
		),
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:02:00"),
		),
	}
	input := make(chan types.Item)
	output := make(chan types.Item)

	receiver := &evictWindowTestReceiver{
		ch: input,
	}

	emitter := &evictWindowTestEmitter{
		ch: output,
	}

	env.Env().TimeCharacteristic = env.IsEventTime
	assigner := assigners.NewGlobalWindow()
	trigger := triggers.NewCountTrigger().Of(2)
	evictor := evictors.NewCountEvictor().Of(1, true)
	w := NewEvictWindow(assigner, trigger, evictor)
	w.SetReduceFunc(&evictWindowTestReduceFunc{})

	var wg sync.WaitGroup

	go func() {
		for _, record := range items {
			input <- record
		}
		close(input)
	}()

	var got []types.Item
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			i, ok := <-output
			if !ok {
				break
			}
			got = append(got, i)
		}
	}()

	// wait until all item is emit to output
	w.Run(receiver, emitter)
	wg.Wait()

	want := []types.Item{
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:00:00"),
			map[string]interface{}{
				"A": 2,
				"B": 3,
			},
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:00:01"),
			map[string]interface{}{
				"A": 3,
				"B": 5,
			},
		),
		types.NewMapRecord(
			utils.ParseTimeMilli("2018-10-15 18:00:59.999"),
			map[string]interface{}{
				"A": 4,
				"B": 7,
			},
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:01:00"),
			map[string]interface{}{
				"A": 5,
				"B": 9,
			},
		),
	}

	if diff := cmp.Diff(got, want); diff != "" {
		t.Error(diff)
	}
}
