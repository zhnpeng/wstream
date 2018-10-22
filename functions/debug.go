package functions

import "github.com/wandouz/wstream/types"

type DebugFunc interface {
	Debug(record types.Record)
}
