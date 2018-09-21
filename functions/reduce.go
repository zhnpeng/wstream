package functions

import (
	"github.com/wandouz/wstream/types"
)

/*
ReduceFunc is a "rolling" reduce
*/
type ReduceFunc interface {
	InitialAccmulator() types.Record
	Reduce(a, b types.Record) (o types.Record)
}
