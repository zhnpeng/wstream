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
	k       []interface{}
	emitter Emitter
}

func NewWindowEmitter(t time.Time, k []interface{}, emitter Emitter) *WindowEmitter {
	return &WindowEmitter{
		t:       t,
		k:       k,
		emitter: emitter,
	}
}

func (e *WindowEmitter) Emit(item types.Item) {
	interItem := item.(types.InternalItem)
	interItem.SetTime(e.t)
	interItem.SetKey(e.k)
	e.emitter.Emit(interItem)
}
