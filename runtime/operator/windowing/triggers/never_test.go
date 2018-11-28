package triggers

import (
	"testing"
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
)

func TestNeverTrigger_Functions(t *testing.T) {
	trigger := &NeverTrigger{}
	w := windows.Window{}
	ctx1 := &mockCTC{}

	got := trigger.OnItem(nil, time.Time{}, w, ctx1)
	if got != CONTINUE {
		t.Errorf("got = %v, want %v", got, CONTINUE)
	}

	got = trigger.OnProcessingTime(time.Time{}, w)
	if got != CONTINUE {
		t.Errorf("got = %v, want %v", got, CONTINUE)
	}

	got = trigger.OnEventTime(time.Time{}, w)
	if got != CONTINUE {
		t.Errorf("got = %v, want %v", got, CONTINUE)
	}
}
