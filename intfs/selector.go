package intfs

import "github.com/wandouz/wstream/types"

type Selector interface {
	Select(record types.Record, size int) int
	New() Selector
}
