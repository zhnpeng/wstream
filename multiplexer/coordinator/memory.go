package coordinator

import (
	"github.com/zhnpeng/wstream/multiplexer/common"
	"sync"
)

var memoryCoordinators = map[string]*MemoryCoordinator{}
var mutex = sync.Mutex{}

func GetOrCreateMemoryCoordinatorConsumer(id string) common.MessageQueue {
	mutex.Lock()
	defer mutex.Unlock()
	coord := memoryCoordinators[id]
	if coord == nil {
		coord = NewMemoryCoordinator(id)
		memoryCoordinators[id] = coord
	}
	return coord.output
}

func GetOrCreateMemoryCoordinatorProducer(id string) common.MessageQueue {
	mutex.Lock()
	defer mutex.Unlock()
	coord := memoryCoordinators[id]
	if coord == nil {
		coord = NewMemoryCoordinator(id)
		memoryCoordinators[id] = coord
	}
	return coord.input
}

type MemoryCoordinator struct {
	id string

	input  common.MessageQueue
	output common.MessageQueue
}

func NewMemoryCoordinator(id string) *MemoryCoordinator {
	inputQueue := make(common.MessageQueue)
	outputQueue := make(common.MessageQueue)

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
	mutex.Lock()
	defer mutex.Unlock()
	delete(memoryCoordinators, c.id)
}