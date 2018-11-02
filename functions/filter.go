package functions

import "github.com/wandouz/wstream/types"

type Filter interface {
	Filter(record types.Record) bool
}
