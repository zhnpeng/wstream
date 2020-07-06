package assigners

import (
	"reflect"
	"testing"
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing/triggers"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
	"github.com/zhnpeng/wstream/types"
	"github.com/zhnpeng/wstream/utils"
)

func TestTumblingEventTimeWindow(t *testing.T) {
	assigner := NewTumblingEventTimeWindow(60, 8)

	dt := utils.ParseTime("2018-11-27 18:01:01")
	got := assigner.AssignWindows(&types.MapRecord{T: dt}, time.Time{})
	want := []windows.Window{
		windows.NewTimeWindow(utils.ParseTime("2018-11-27 18:01:00"), utils.ParseTime("2018-11-27 18:02:00")),
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
