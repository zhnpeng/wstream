package execution

import (
	"sync"
)

/*
Task is a logical node aginst to stream node
1. RescaleNodes should only exists in rescaling task, for example keyby
2. The number of RescaleNodes should lte than upstream task's parallelism
3. Each RescaleNode should accept more than one upstream taks's output as input
4. For better performance RescaleNodes is a slice, because every node is an execution unit,
   if there is only one RescaleNode there may be a problem for performance
*/
type Task struct {
	RescaleNodes   []*Node
	BroadcastNodes []*Node
}

func (t *Task) Run() {
	var wg sync.WaitGroup
	for _, n := range t.RescaleNodes {
		wg.Add(1)
		go func(sn *Node) {
			defer wg.Done()
			sn.Run()
		}(n)
	}
	for _, n := range t.BroadcastNodes {
		wg.Add(1)
		go func(bn *Node) {
			defer wg.Done()
			bn.Run()
		}(n)
	}
	wg.Wait()
}
