package functions

import (
	"github.com/wandouz/wstream/streaming/sio"
	"github.com/wandouz/wstream/types"
)

type MapFunc interface {
	Map(item types.Item) (Out types.Item)
}

type Map struct {
	Function MapFunc
}

func (f *Map) Run(item types.Item, emitter *sio.Emitter) {
	o := f.Function.Map(item)
	emitter.Emit(o)
}

func (f *Map) Accmulator() types.Item {
	return nil
}
