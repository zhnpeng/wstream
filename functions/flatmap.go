package functions

import (
	"github.com/wandouz/wstream/types"
)

type FlatMap interface {
	FlatMap(record types.Record, emitter Emitter)
}
