package funcintfs

import "github.com/zhnpeng/wstream/types"

type Filter interface {
	Filter(record types.Record) bool
}
