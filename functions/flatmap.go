package functions

import (
	"github.com/wandouz/wstream/types"
)

type FlatMapFunc interface {
	FlatMap(item types.Item, emitter Emitter)
}

type FlatMap struct {
	Function FlatMapFunc
}

func (f *FlatMap) Run(item types.Item, emitter Emitter) {
	f.Function.FlatMap(item, emitter)
}

func (f *FlatMap) Accmulator() types.Item {
	return nil
}