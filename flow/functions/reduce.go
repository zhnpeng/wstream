package functions

import (
	"github.com/wandouz/wstream/types"
)

/*
ReduceFunc is a "rolling" reduce
*/
type ReduceFunc interface {
	Accmulator() types.Item
	Reduce(a, b types.Item) (o types.Item)
}

type Reduce struct {
	Function   ReduceFunc
	accmulator types.Item
}

func NewReduce(function ReduceFunc) *Reduce {
	return &Reduce{
		Function:   function,
		accmulator: function.Accmulator(),
	}
}

func (f *Reduce) Accmulator() types.Item {
	return f.Function.Accmulator()
}

func (f *Reduce) Run(item types.Item, emitter Emitter) {
	f.accmulator = f.Function.Reduce(f.accmulator, item)
	emitter.Emit(f.accmulator)
}
