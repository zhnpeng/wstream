package functions

import (
	"github.com/wandouz/wstream/types"
)

type UserDefinedFunction interface {
	Run(item types.Item, emitter Emitter)
}
