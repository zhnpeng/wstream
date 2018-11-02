package functions

import "github.com/wandouz/wstream/types"

type Debug interface {
	Debug(record types.Record)
}
