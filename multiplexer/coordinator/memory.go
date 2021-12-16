package coordinator

import (
	"github.com/zhnpeng/wstream/multiplexer"
	"sync"
)

var memoryCoordinators = sync.Map{}

func GetOrCreateMemoryCoordinator(id string) *MemoryCoordinator {
	ret, _ := memoryCoordinators.LoadOrStore(id, NewMemoryCoordinator(id))
	return ret.(*MemoryCoordinator)
}

func GetOrCreateMemoryCoordinatorConsumer(id string) multiplexer.MessageQueue {
	coord := GetOrCreateMemoryCoordinator(id)
	return coord.input
}

func GetOrCreateMemoryCoordinatorProducer(id string) multiplexer.MessageQueue {
	coord := GetOrCreateMemoryCoordinator(id)
	return coord.output
}

type MemoryCoordinator struct {
	id string

	input multiplexer.MessageQueue
	output multiplexer.MessageQueue
}

func NewMemoryCoordinator(id string) *MemoryCoordinator {
	inputQueue := make(multiplexer.MessageQueue)
	outputQueue := make(multiplexer.MessageQueue)

	corrd := &MemoryCoordinator{
		id: id,
		input: inputQueue,
		output: outputQueue,
	}
	go corrd.run()
	return corrd
}

func (c *MemoryCoordinator) run() {
	defer c.dispose()
	for {
		select {
		case msg, ok := <- c.input:
			if !ok {
				close(c.output)
				return
			}
			c.output <- msg
		}
	}
}

func (c *MemoryCoordinator) dispose() {
	memoryCoordinators.Delete(c.id)
}