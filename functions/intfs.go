package functions

import "github.com/wandouz/wstream/types"

type Emitter interface {
	Emit(item types.Item)
}
