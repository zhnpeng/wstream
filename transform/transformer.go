package transform

import (
	"fmt"

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

	sg.BFSBoth(0, buildTaskGraph(sg, tg))
	return
}

func (t *Transformer) BFSAll(g Iterator, v int) {
	do := func(v, w int, c int64) {
		fmt.Println(v, w)
	}
	visited := make([]bool, g.Order())
	visited[v] = true
	for queue := []int{v}; len(queue) > 0; {
		v := queue[0]
		queue = queue[1:]
		g.VisitBoth(
			v,
			func(w int, c int64) (skip bool) {
				if visited[w] {
					return
				}
				visited[w] = true
				queue = append(queue, w)
				return
			},
			func(w int, c int64) (skip bool) {
				do(v, w, c)
				if visited[w] {
					return
				}
				visited[w] = true
				queue = append(queue, w)
				return
			},
		)
	}
}
