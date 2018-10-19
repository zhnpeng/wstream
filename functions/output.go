package functions

import "github.com/wandouz/wstream/types"

type OutputFunc interface {
	Output(record types.Record)
}
