package intfs

import "github.com/wandouz/wstream/types"

type Emitter interface {
	Length() int
	Emit(item types.Item)
	EmitTo(index int, item types.Item) error
}
