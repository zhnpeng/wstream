package functions

import (
	"github.com/zhnpeng/wstream/types"
)

type FlatMap interface {
	FlatMap(record types.Record, emitter Emitter)
}
