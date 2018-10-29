package operator

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/sirupsen/logrus"

	"github.com/spf13/cast"
	"github.com/wandouz/wstream/env"
	"github.com/wandouz/wstream/runtime/operator/windowing/assigners"
	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/types"
	"github.com/wandouz/wstream/utils"
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

func (e *windowtestEmitter) Emit(item types.Item) {
	e.ch <- item
}

func (e *windowtestEmitter) EmitTo(index int, item types.Item) error {
	return nil
}

func (e *windowtestEmitter) Dispose() {
	close(e.ch)
}

type windowTestReduceFunc struct {
}

func (f *windowTestReduceFunc) Reduce(x types.Record, y types.Record) types.Record {
	if x == nil {
		return types.NewRawMapRecord(map[string]interface{}{
			"A": cast.ToInt(y.Get("A")),
			"B": cast.ToInt(y.Get("B")),
		})
	}
	return types.NewRawMapRecord(map[string]interface{}{
		"A": int(math.Max(cast.ToFloat64(x.Get("A")), cast.ToFloat64(y.Get("A")))),
		"B": cast.ToInt(x.Get("B")) + cast.ToInt(y.Get("B")),
	})
}

func TestWindow_Run_Tumbling_EventTime_Window(t *testing.T) {
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

	receiver := &windowTestReceiver{
		ch: input,
	}

	emitter := &windowtestEmitter{
		ch: output,
	}

	env.Env().TimeCharacteristic = env.IsEventTime
	assigner := assigners.NewTumblingEventTimeWindow(60, 0)
	trigger := triggers.NewEventTimeTrigger()
	w := NewWindow(assigner, trigger)
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
	wg.Wait()

	want := []types.Item{
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:00:00"),
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:00:00"),
			map[string]interface{}{
				"A": 3,
				"B": 6,
			},
		),
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:01:00"),
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

func TestWindow_Run_Sliding_EventTime_Window(t *testing.T) {
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
			utils.ParseTime("2018-10-15 18:00:02"),
			map[string]interface{}{
				"A": 3,
				"B": 3,
			},
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:00:03"),
			map[string]interface{}{
				"A": 4,
				"B": 4,
			},
		),
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:00:06"),
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

	env.Env().TimeCharacteristic = env.IsEventTime
	assigner := assigners.NewSlidingEventTimeWindoww(2, 1, 0)
	trigger := triggers.NewEventTimeTrigger()
	w := NewWindow(assigner, trigger)
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
	wg.Wait()

	want := []types.Item{
		types.NewWatermark(
			utils.ParseTime("2018-10-15 17:59:59"),
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 17:59:59"),
			map[string]interface{}{
				"A": 1,
				"B": 1,
			},
		),
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:00:00"),
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:00:00"),
			map[string]interface{}{
				"A": 2,
				"B": 3,
			},
		),
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:00:01"),
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:00:01"),
			map[string]interface{}{
				"A": 3,
				"B": 5,
			},
		),
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:00:02"),
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:00:02"),
			map[string]interface{}{
				"A": 4,
				"B": 7,
			},
		),
		types.NewWatermark(
			utils.ParseTime("2018-10-15 18:00:03"),
		),
		types.NewMapRecord(
			utils.ParseTime("2018-10-15 18:00:03"),
			map[string]interface{}{
				"A": 4,
				"B": 4,
			},
		),
	}

	if diff := cmp.Diff(got, want); diff != "" {
		t.Error(diff)
	}
}

func TestWindow_Run_Tumbling_ProcessingTime_Window(t *testing.T) {
	items := []types.Item{
		types.NewMapRecord(
			time.Time{},
			map[string]interface{}{
				"A": 1,
				"B": 1,
			},
		),
		types.NewMapRecord(
			time.Time{},
			map[string]interface{}{
				"A": 2,
				"B": 2,
			},
		),
		types.NewMapRecord(
			time.Time{},
			map[string]interface{}{
				"A": 3,
				"B": 3,
			},
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

	env.Env().TimeCharacteristic = env.IsEventTime
	assigner := assigners.NewTumblingProcessingTimeWindow(1, 0)
	trigger := triggers.NewProcessingTimeTrigger()
	w := NewWindow(assigner, trigger)
	w.SetReduceFunc(&windowTestReduceFunc{})

	var wg sync.WaitGroup

	go func() {
		for _, record := range items {
			input <- record
			time.Sleep(time.Second + 50*time.Millisecond)
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
			time.Time{},
			map[string]interface{}{
				"A": 1,
				"B": 1,
			},
		),
		types.NewMapRecord(
			time.Time{},
			map[string]interface{}{
				"A": 2,
				"B": 2,
			},
		),
		types.NewMapRecord(
			time.Time{},
			map[string]interface{}{
				"A": 3,
				"B": 3,
			},
		),
	}

	if len(got) != 3 {
		t.Errorf("length of got not right got %d, want %d", len(got), 3)
	}
	for i, g := range got {
		if g.Time().Equal(want[i].Time()) {
			logrus.Errorf("got items should not has time: %v", g.Time())
		}
		g.SetTime(time.Time{})
		if diff := cmp.Diff(g, want[i]); diff != "" {
			t.Error(diff)
		}
	}
}

func TestWindow_Run_Sliding_ProcessingTime_Window(t *testing.T) {
	items := []types.Item{
		types.NewMapRecord(
			time.Time{},
			map[string]interface{}{
				"A": 1,
				"B": 1,
			},
		),
		types.NewMapRecord(
			time.Time{},
			map[string]interface{}{
				"A": 2,
				"B": 2,
			},
		),
		types.NewMapRecord(
			time.Time{},
			map[string]interface{}{
				"A": 3,
				"B": 3,
			},
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

	env.Env().TimeCharacteristic = env.IsEventTime
	assigner := assigners.NewSlidingProcessingTimeWindow(2, 1, 0)
	trigger := triggers.NewProcessingTimeTrigger()
	w := NewWindow(assigner, trigger)
	w.SetReduceFunc(&windowTestReduceFunc{})

	var wg sync.WaitGroup

	go func() {
		for _, record := range items {
			input <- record
			time.Sleep(time.Second + 50*time.Millisecond)
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

	// for _, g := range got {
	// 	logrus.Info(g)
	// }

	want := []types.Item{
		types.NewMapRecord(
			time.Time{},
			map[string]interface{}{
				"A": 1,
				"B": 1,
			},
		),
		types.NewMapRecord(
			time.Time{},
			map[string]interface{}{
				"A": 2,
				"B": 3,
			},
		),
		types.NewMapRecord(
			time.Time{},
			map[string]interface{}{
				"A": 3,
				"B": 5,
			},
		),
	}

	if len(got) != 3 {
		t.Errorf("length of got not right got %d, want %d", len(got), 3)
	}
	for i, g := range got {
		if g.Time().Equal(want[i].Time()) {
			logrus.Errorf("got items should not has time: %v", g.Time())
		}
		g.SetTime(time.Time{})
		if diff := cmp.Diff(g, want[i]); diff != "" {
			t.Error(diff)
		}
	}
}
