package functions

import (
	"github.com/wandouz/wstream/types"
)

type Map interface {
	Map(record types.Record) (Out types.Record)
}
