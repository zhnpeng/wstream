package functions

import "github.com/wandouz/wstream/types"

type ApplyFunc interface {
	Apply(records []types.Record, emitter Emitter) types.Record
}
