package functions

import (
	"github.com/wandouz/wstream/types"
)

type MapFunc interface {
	Map(record types.Record) (Out types.Record)
}
