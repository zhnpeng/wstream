package functions

import "github.com/zhnpeng/wstream/types"

type Emitter interface {
	Emit(item types.Item)
}
