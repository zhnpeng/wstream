package functions

import (
	"github.com/wandouz/wstream/types"
)

/*
Reduce run after non-windowded datastream is a "rolling" reduce
*/
type Reduce interface {
	Reduce(a, b types.Record) (o types.Record)
}
