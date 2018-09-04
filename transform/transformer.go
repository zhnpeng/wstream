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

func buildTaskGraph(sg *stream.StreamGraph, tg *task.TaskGraph) func(v, w int, c int64) {
	return func(v, w int, c int64) {
		// streamSrcNode := sg.GetStream(v)
		// streamDstNode := sg.GetStream(w)
		//TODO: transform streamNode to taskNode
		// then add taskNodes to task graph
		// tg.AddTaskEdge()
	}
}

func (t *Transformer) Stream2Execution(sg *stream.StreamGraph) (tg *task.TaskGraph) {
	tg = task.NewTaskGraph()
	if sg.Length() == 0 {
		return
	}

	sg.BFSAll(0, buildTaskGraph(sg, tg))
	return
}
