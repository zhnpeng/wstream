package functions

import (
	"github.com/zhnpeng/wstream/types"
)

type Map interface {
	Map(record types.Record) (Out types.Record)
}
