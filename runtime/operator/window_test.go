package operator

import (
	"testing"

	"github.com/wandouz/wstream/global"
	"github.com/wandouz/wstream/runtime/operator/windowing/assigners"
	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/types"
)

type testReceiver struct {
	ch chan types.Item
}

func (r *testReceiver) Next() <-chan types.Item {
	return r.ch
}

type testEmitter struct {
	ch chan types.Item
}

func (e *testEmitter) Length() int {
	return 1
}

func (e *testEmitter) Emit(item types.Item) error {
	e.ch <- item
	return nil
}

func (e *testEmitter) EmitTo(index int, item types.Item) error {
	return nil
}

func TestWindow_Run_Tumbling_EventTime_Window(t *testing.T) {

	receiver := &testReceiver{
		ch: make(chan types.Item),
	}

	emitter := &testEmitter{
		ch: make(chan types.Item, 100),
	}

	global.ENV.TimeCharacteristic = global.IsEventTime
	assigner := assigners.NewTumblingEventTimeWindow(60, 0)
	trigger := triggers.NewEventTimeTrigger()
	w := NewWindow(assigner, trigger)

	// source := make(execution.Edge)
	w.Run(receiver, emitter)
}
