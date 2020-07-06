package assigners

import (
	"reflect"
	"testing"
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing/triggers"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
)

func TestGlobalWindow_Functions(t *testing.T) {
	assigner := NewGlobalWindow()

	if !reflect.DeepEqual(assigner.AssignWindows(nil, time.Time{}), []windows.Window{windows.GetGlobalWindow()}) {
		t.Errorf("AssignWindows got = %v, want %v", assigner.AssignWindows(nil, time.Time{}), []windows.Window{windows.GetGlobalWindow()})
	}

	if !reflect.DeepEqual(assigner.GetDefaultTrigger(), &triggers.NeverTrigger{}) {
		t.Errorf("GetDefaultTrigger got = %v, want %v", assigner.GetDefaultTrigger(), &triggers.NeverTrigger{})
	}

	if assigner.IsEventTime() != false {
		t.Errorf("IsEventTime got = %v, want %v", assigner.IsEventTime(), false)
	}
}
