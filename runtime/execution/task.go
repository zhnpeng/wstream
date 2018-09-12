package execution

import (
	"sync"
)

type Task struct {
	RescaleNode    Node
	BroadcastNodes []Node
}

func (t *Task) Run() {
	var wg sync.WaitGroup
	if t.RescaleNode != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.RescaleNode.Run()
		}()
	}
	for _, n := range t.BroadcastNodes {
		wg.Add(1)
		go func(bn Node) {
			defer wg.Done()
			bn.Run()
		}(n)
	}
	wg.Wait()
}
