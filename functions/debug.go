package functions

import "github.com/zhnpeng/wstream/types"

type Debug interface {
	Debug(record types.Record)
}
