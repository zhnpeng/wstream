package intfs

import "github.com/zhnpeng/wstream/types"

type Selector interface {
	Select(record types.Record, size int) int
	New() Selector
}
