package multiplexer

import (
	"github.com/zhnpeng/wstream/multiplexer/common"
	"github.com/zhnpeng/wstream/multiplexer/coordinator"
)

type SocketProducer struct {
	ID     string
	Sender common.MessageQueue
}

func NewSocketProducer(id string, params coordinator.SocketProducerParams) *SocketProducer {
	output := coordinator.GetOrCreateSocketCoordinatorProducer(id, params)
	return &SocketProducer{
		ID: id,
		Sender: output,
	}
}

func (p *SocketProducer) Close() {
	close(p.Sender)
}

type SocketConsumer struct {
	ID string

	Receiver common.MessageQueue
}

func NewSocketConsumer(id string, params coordinator.SocketConsumerParams) *SocketConsumer {
	input := coordinator.GetOrCreateSocketCoordinatorConsumer(id, params)
	return &SocketConsumer{
		ID: id,
		Receiver: input,
	}
}

