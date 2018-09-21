package functions

import "github.com/wandouz/wstream/types"

type FilterFunc interface {
	Filter(record types.Record) bool
}
