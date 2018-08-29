package task

import "github.com/wandouz/wstream/runtime/execution"

type Task struct {
	nodes    []execution.Node
	taskNode *taskNode
}

func (t *Task) SetTaskNode(tn *taskNode) {
	t.taskNode = tn
}

func (t *Task) GetTaskNode() *taskNode {
	return t.taskNode
}
