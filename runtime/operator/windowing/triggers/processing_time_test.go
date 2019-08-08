package triggers

import (
	"testing"
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
)

func TestProcessingTimeTrigger_Functions(t *testing.T) {
	trigger := NewProcessingTimeTrigger()
	w := windows.Window{}
	ctx1 := &mockCTC{size: 10}

	got := trigger.OnItem(nil, time.Time{}, w, ctx1)
	if got != CONTINUE {
		t.Errorf("got = %v, want %v", got, CONTINUE)
	}

	got = trigger.OnProcessingTime(time.Time{}, w)
	if got != FIRE {
		t.Errorf("got = %v, want %v", got, FIRE)
	}

	got = trigger.OnEventTime(time.Time{}, w)
	if got != CONTINUE {
		t.Errorf("got = %v, want %v", got, CONTINUE)
	}
}
