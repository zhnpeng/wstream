package triggers

import (
	"testing"
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type mockCTC struct {
	size      int
	eventTime time.Time
}

func (m *mockCTC) RegisterProcessingTimer(t time.Time) {}
func (m *mockCTC) RegisterEventTimer(t time.Time)      {}
func (m *mockCTC) GetCurrentEventTime() time.Time      { return m.eventTime }
func (m *mockCTC) WindowSize() int                     { return m.size }

func TestCountTrigger_Functions(t *testing.T) {
	trigger := NewCountTrigger().Of(11)
	w := windows.Window{}
	ctx1 := &mockCTC{size: 10}
	ctx2 := &mockCTC{size: 11}
	ctx3 := &mockCTC{size: 12}

	got := trigger.OnItem(&types.MapRecord{}, time.Time{}, w, ctx1)
	if got != CONTINUE {
		t.Errorf("got = %v, want %v", got, CONTINUE)
	}

	got = trigger.OnItem(&types.MapRecord{}, time.Time{}, w, ctx2)
	if got != FIRE {
		t.Errorf("got = %v, want %v", got, FIRE)
	}

	got = trigger.OnItem(&types.MapRecord{}, time.Time{}, w, ctx3)
	if got != FIRE {
		t.Errorf("got = %v, want %v", got, FIRE)
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
