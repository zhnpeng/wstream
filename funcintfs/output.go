package funcintfs

import "github.com/zhnpeng/wstream/types"

type Output interface {
	Output(record types.Record)
}
