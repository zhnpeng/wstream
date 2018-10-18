package operator

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/spf13/cast"
	"github.com/wandouz/wstream/env"
	"github.com/wandouz/wstream/helpers"
	"github.com/wandouz/wstream/runtime/operator/windowing/assigners"
	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/types"
)

type windowTestReceiver struct {
	ch chan types.Item
}

func (r *windowTestReceiver) Next() <-chan types.Item {
	return r.ch
}

type windowtestEmitter struct {
	ch chan types.Item
}

func (e *windowtestEmitter) Length() int {
	return 1
}

func (e *windowtestEmitter) Emit(item types.Item) error {
	e.ch <- item
	return nil
}

func (e *windowtestEmitter) EmitTo(index int, item types.Item) error {
	return nil
}

type windowTestReduceFunc struct {
}

func (f *windowTestReduceFunc) Reduce(x types.Record, y types.Record) types.Record {
	ret := map[string]interface{}{
		"A": int(math.Max(cast.ToFloat64(x.Get("A")), cast.ToFloat64(y.Get("A")))),
		"B": cast.ToInt(x.Get("B")) + cast.ToInt(y.Get("B")),
	}
	return types.NewMapRecord(time.Time{}, ret)
}

func TestWindow_Run_Tumbling_EventTime_Window(t *testing.T) {
	items := []types.Item{
		types.NewWatermark(
			helpers.TimeParse("2018-10-15 18:00:00"),
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-10-15 18:00:00"),
			map[string]interface{}{
				"A": 1,
				"B": 1,
			},
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-10-15 18:00:01"),
			map[string]interface{}{
				"A": 2,
				"B": 2,
			},
		),
		types.NewMapRecord(
			helpers.MilliTimeParse("2018-10-15 18:00:59.999"),
			map[string]interface{}{
				"A": 3,
				"B": 3,
			},
		),
		types.NewWatermark(
			helpers.TimeParse("2018-10-15 18:01:00"),
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-10-15 18:01:00"),
			map[string]interface{}{
				"A": 4,
				"B": 4,
			},
		),
		types.NewMapRecord(
			helpers.MilliTimeParse("2018-10-15 18:01:59.999"),
			map[string]interface{}{
				"A": 5,
				"B": 5,
			},
		),
		types.NewWatermark(
			helpers.TimeParse("2018-10-15 18:02:00"),
		),
	}
	input := make(chan types.Item)
	output := make(chan types.Item)

	receiver := &windowTestReceiver{
		ch: input,
	}

	emitter := &windowtestEmitter{
		ch: output,
	}

	env.ENV.TimeCharacteristic = env.IsEventTime
	assigner := assigners.NewTumblingEventTimeWindow(60, 0)
	trigger := triggers.NewEventTimeTrigger()
	w := NewWindow(assigner, trigger).(*Window)
	w.SetReduceFunc(&windowTestReduceFunc{})

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
	close(output)
	wg.Wait()

	want := []types.Item{
		types.NewMapRecord(
			helpers.TimeParse("2018-10-15 18:00:00"),
			map[string]interface{}{
				"A": 3,
				"B": 6,
			},
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-10-15 18:01:00"),
			map[string]interface{}{
				"A": 5,
				"B": 9,
			},
		),
		types.NewWatermark(
			helpers.TimeParse("2018-10-15 18:01:00"),
		),
	}

	if diff := cmp.Diff(got, want); diff != "" {
		t.Error(diff)
	}
}
