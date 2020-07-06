package assigners

import (
	"reflect"
	"testing"
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing/triggers"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
	"github.com/zhnpeng/wstream/utils"
)

type mockAC struct {
	processingTime time.Time
}

func (m *mockAC) GetCurrentProcessingTime() time.Time {
	return m.processingTime
}

func TestTumblingProcessingTimeWindow(t *testing.T) {
	assigner := NewTumblingProcessingTimeWindow(60, 8)

	currentTime := utils.ParseTime("2018-11-27 18:01:01")
	got := assigner.AssignWindows(nil, currentTime)
	want := []windows.Window{
		windows.NewTimeWindow(utils.ParseTime("2018-11-27 18:01:00"), utils.ParseTime("2018-11-27 18:02:00")),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("AssignWindows got = %v, want %v", got, want)
	}

	if !reflect.DeepEqual(assigner.GetDefaultTrigger(), triggers.NewProcessingTimeTrigger()) {
		t.Errorf("GetDefaultTrigger got = %v, want %v", assigner.GetDefaultTrigger(), triggers.NewProcessingTimeTrigger())
	}

	if assigner.IsEventTime() != false {
		t.Errorf("IsEventTime got = %v, want %v", assigner.IsEventTime(), false)
	}
}
