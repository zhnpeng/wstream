package task

import (
	"sync"

	"github.com/wandouz/wstream/runtime/execution"
)

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

func (t *Task) Run() {
	var wg sync.WaitGroup
	for _, node := range t.nodes {
		wg.Add(1)
		go func(subtask execution.Node) {
			defer wg.Done()
			subtask.Run()
		}(node)
	}
	wg.Wait()
}
