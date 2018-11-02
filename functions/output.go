package functions

import "github.com/wandouz/wstream/types"

type Output interface {
	Output(record types.Record)
}
