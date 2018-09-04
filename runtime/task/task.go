package task

import (
	"sync"

	"github.com/wandouz/wstream/runtime/execution"
)

type Task struct {
	rescaleNode    *execution.RescaleNode
	broadcastNodes []*execution.BroadcastNode
	taskNode       *taskNode
}

func (t *Task) RescaleNode() *execution.RescaleNode {
	return t.rescaleNode
}

func (t *Task) BroadcastNodes() []*execution.BroadcastNode {
	return t.broadcastNodes
}

func (t *Task) SetTaskNode(tn *taskNode) {
	t.taskNode = tn
}

func (t *Task) GetTaskNode() *taskNode {
	return t.taskNode
}

func (t *Task) Run() {
	var wg sync.WaitGroup
	if t.rescaleNode != nil {
		wg.Add(1)
		go func(n *execution.RescaleNode) {
			defer wg.Done()
			n.Run()
		}(t.rescaleNode)
	}
	for _, node := range t.broadcastNodes {
		wg.Add(1)
		go func(n *execution.BroadcastNode) {
			defer wg.Done()
			n.Run()
		}(node)
	}
	wg.Wait()
}
