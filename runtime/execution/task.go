package execution

import "sync"

type Task struct {
	Nodes []Node
}

func (t *Task) Run() {
	var wg sync.WaitGroup
	for _, n := range t.Nodes {
		wg.Add(1)
		go func(bn Node) {
			defer wg.Done()
			bn.Run()
		}(n)
	}
	wg.Wait()
}
