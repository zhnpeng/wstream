package windows

import (
	"testing"
	"time"

	"github.com/wandouz/wstream/utils"
)

func TestNew(t *testing.T) {
	start := utils.ParseTime("2018-11-28 16:00:00")
	end := utils.ParseTime("2018-11-28 16:01:00")
	w := New(start, end)

	if !w.Start().Equal(start) {
		t.Errorf("got = %v, want %v", w.Start(), start)
	}
	if !w.End().Equal(end) {
		t.Errorf("got = %v, want %v", w.End(), end)
	}

	if !w.MaxTimestamp().Equal(end.Add(-1 * time.Second)) {
		t.Errorf("got = %v, want %v", w.MaxTimestamp(), end.Add(-1*time.Second))
	}
}
