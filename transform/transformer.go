package transform

import (
	"github.com/wandouz/wstream/flow/stream"
	"github.com/wandouz/wstream/runtime/execution"
)

type Transformer struct {
}

func New() *Transformer {
	return &Transformer{}
}

func (t *Transformer) Stream2Execution(sg *stream.StreamGraph) (eg *execution.ExecutionGraph) {
	eg = &execution.ExecutionGraph{}
	if sg.Length() == 0 {
		return
	}
	stmNode := sg.GetVertex(0)
	if stmNode == nil {
		return
	}
	stm := stmNode.Value
	switch stm.(type) {
	case *stream.DataStream:
	case *stream.KeyedStream:
	}
	return
}
