package task

import (
	"github.com/wandouz/wstream/wio"
)

type Task struct {
	subTasks []*SubTask
}

//SubTask represents parallel sub task of a task
//A task with parallism 1 has only 1 sub task
type SubTask struct {
	task *Task
}

type ExecutionNode struct {
	Pipe     *wio.Pipe
	InEdges  []*ExecutionNode
	OutEdges []*ExecutionNode
}
