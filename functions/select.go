package functions

import "github.com/zhnpeng/wstream/types"

type Select interface {
	Select(record types.Record, size int) (index int)
}
