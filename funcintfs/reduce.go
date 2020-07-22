package funcintfs

import (
	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
	"github.com/zhnpeng/wstream/types"
)

/*
WindowReduce run after a windowed datastream
*/
type WindowReduce interface {
	// Accmulater return initial accmulater for reduce
	Accmulater(window windows.Window, record types.Record) (acc types.Record)
	Reduce(a, b types.Record) (o types.Record)
}

/*
Reduce run after non-windowded datastream, it is a "rolling" reduce
*/
type Reduce interface {
	Accmulater(record types.Record) (acc types.Record)
	Reduce(a, b types.Record) (acc types.Record)
}
