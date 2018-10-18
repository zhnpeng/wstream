package operator

import (
	"time"

	"github.com/wandouz/wstream/types"
)

// WindowEmitter is proxy of normal emitter
// used to overwrite record's time to window's start time
// before record is emit to downstream operator
type WindowEmitter struct {
	t       time.Time
	emitter Emitter
}

func NewWindowEmitter(t time.Time, emitter Emitter) *WindowEmitter {
	return &WindowEmitter{
		t:       t,
		emitter: emitter,
	}
}

func (e *WindowEmitter) Emit(item types.Item) error {
	item.SetTime(e.t)
	e.emitter.Emit(item)
	return nil
}
