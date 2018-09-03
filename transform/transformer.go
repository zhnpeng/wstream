package transform

import (
	"github.com/wandouz/wstream/runtime/task"
	"github.com/wandouz/wstream/stream"
)

type Transformer struct {
}

func New() *Transformer {
	return &Transformer{}
}

func doBuildTaskGraph(sg *stream.StreamGraph, tg *task.TaskGraph) func(v, w int, c int64) {
	return func(v, w int, c int64) {
		streamNode := sg.GetStream(w)
		if streamNode != nil {
			//TODO: transform streamNode to taskNode
		}
	}
}

func (t *Transformer) Stream2Execution(sg *stream.StreamGraph) (tg *task.TaskGraph) {
	tg = task.NewTaskGraph()
	if sg.Length() == 0 {
		return
	}

	sg.BFS(0, doBuildTaskGraph(sg, tg))
	return
}
