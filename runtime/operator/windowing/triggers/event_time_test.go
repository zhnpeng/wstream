package triggers

import (
	"testing"
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/utils"
)

func TestEventTimeTrigger_Functions(t *testing.T) {
	trigger := NewEventTimeTrigger()
	start := utils.ParseTime("2018-11-28 16:00:00")
	end := utils.ParseTime("2018-11-28 16:01:00")
	w := windows.New(start, end)
	ctx1 := &mockCTC{eventTime: utils.ParseTime("2018-11-28 16:00:00")}
	ctx2 := &mockCTC{eventTime: utils.ParseTime("2018-11-28 16:01:00")}
	ctx3 := &mockCTC{eventTime: utils.ParseTime("2018-11-28 16:02:00")}

	got := trigger.OnItem(nil, time.Time{}, w, ctx1)
	if got != CONTINUE {
		t.Errorf("got = %v, want %v", got, CONTINUE)
	}

	got = trigger.OnItem(nil, time.Time{}, w, ctx2)
	if got != FIRE {
		t.Errorf("got = %v, want %v", got, FIRE)
	}

	got = trigger.OnItem(nil, time.Time{}, w, ctx3)
	if got != FIRE {
		t.Errorf("got = %v, want %v", got, FIRE)
	}

	got = trigger.OnProcessingTime(time.Time{}, w)
	if got != CONTINUE {
		t.Errorf("got = %v, want %v", got, CONTINUE)
	}

	got = trigger.OnEventTime(utils.ParseTime("2018-11-28 16:00:59"), w)
	if got != FIRE {
		t.Errorf("got = %v, want %v", got, FIRE)
	}

	got = trigger.OnEventTime(utils.ParseTime("2018-11-28 16:01:59"), w)
	if got != CONTINUE {
		t.Errorf("got = %v, want %v", got, CONTINUE)
	}
}
