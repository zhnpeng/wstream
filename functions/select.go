package functions

import "github.com/wandouz/wstream/types"

type Select interface {
	Select(record types.Record, size int) (index int)
}
