package multiplexer

import (
	"github.com/zhnpeng/wstream/multiplexer/common"
	"github.com/zhnpeng/wstream/multiplexer/coordinator"
)

type MemoryProducer struct {
	ID     string
	Sender common.MessageQueue
}

func NewMemoryProducer(id string) *MemoryProducer {
	output := coordinator.GetOrCreateMemoryCoordinatorProducer(id)
	return &MemoryProducer{
		ID: id,
		Sender: output,
	}
}

func (p *MemoryProducer) Close() {
	close(p.Sender)
}


type MemoryConsumer struct {
	ID       string
	Receiver common.MessageQueue
}

func NewMemoryConsumer(id string) *MemoryConsumer {
	input := coordinator.GetOrCreateMemoryCoordinatorConsumer(id)
	return &MemoryConsumer{
		ID: id,
		Receiver: input,
	}
}
