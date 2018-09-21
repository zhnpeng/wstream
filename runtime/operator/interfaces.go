package operator

import "github.com/wandouz/wstream/types"

type Emitter interface {
	Emit(item types.Item) error
	EmitTo(index int, item types.Item) error
	Length() int
}
