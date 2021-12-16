package producer

import (
	"github.com/zhnpeng/wstream/multiplexer"
	"github.com/zhnpeng/wstream/multiplexer/coordinator"
)

type MemoryProducer struct {
	ID string
	Sender multiplexer.MessageQueue
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