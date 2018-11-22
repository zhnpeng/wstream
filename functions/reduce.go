package functions

import (
	"github.com/wandouz/wstream/types"
)

/*
Reduce run after non-windowded datastream is a "rolling" reduce
*/
type Reduce interface {
	// Accmulater return initial accmulater for reduce
	Accmulater(a types.Record) (acc types.Record)
	Reduce(a, b types.Record) (o types.Record)
}
