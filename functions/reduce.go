package functions

import (
	"github.com/wandouz/wstream/types"
)

/*
ReduceFunc run after non-windowded datastream is a "rolling" reduce
*/
type ReduceFunc interface {
	Reduce(a, b types.Record) (o types.Record)
}
