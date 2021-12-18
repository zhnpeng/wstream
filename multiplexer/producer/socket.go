package producer

import (
	"github.com/zhnpeng/wstream/multiplexer"
	"github.com/zhnpeng/wstream/multiplexer/coordinator"
)

type SocketProducer struct {
	ID string
	Sender multiplexer.MessageQueue
}

func NewSocketProducer(id string) *SocketProducer {
	output := coordinator.GetOrCreateSocketCoordinatorProducer(id)
	return &SocketProducer{
		ID: id,
		Sender: output,
	}
}

func (p *SocketProducer) Close() {
	close(p.Sender)
}