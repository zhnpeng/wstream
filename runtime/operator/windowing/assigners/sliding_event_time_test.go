package assigners

import (
	"reflect"
	"testing"

	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
	"github.com/wandouz/wstream/utils"
)

func TestSlidingEventTimeWindoww_AssignWindows(t *testing.T) {
	assigner := NewSlidingEventTimeWindoww(120, 60, 8)

	dt := utils.ParseTime("2018-11-27 18:01:01")
	got := assigner.AssignWindows(&types.MapRecord{T: dt}, nil)
	want := []windows.Window{
		windows.NewTimeWindow(utils.ParseTime("2018-11-27 18:01:00"), utils.ParseTime("2018-11-27 18:03:00")),
		windows.NewTimeWindow(utils.ParseTime("2018-11-27 18:00:00"), utils.ParseTime("2018-11-27 18:02:00")),
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("AssignWindows got = %v, want %v", got, want)
	}

	if !reflect.DeepEqual(assigner.GetDefaultTrigger(), triggers.NewEventTimeTrigger()) {
		t.Errorf("GetDefaultTrigger got = %v, want %v", assigner.GetDefaultTrigger(), triggers.NewEventTimeTrigger())
	}

	if assigner.IsEventTime() != true {
		t.Errorf("IsEventTime got = %v, want %v", assigner.IsEventTime(), true)
	}
}
