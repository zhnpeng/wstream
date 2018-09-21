package functions

import (
	"github.com/wandouz/wstream/types"
)

type FlatMapFunc interface {
	FlatMap(record types.Record, emitter Emitter)
}
