package consumer

import (
	"github.com/zhnpeng/wstream/multiplexer"
	"github.com/zhnpeng/wstream/multiplexer/coordinator"
)

type SocketConsumer struct {
	ID string

	Receiver multiplexer.MessageQueue
}

func NewSocketConsumer(id string, params coordinator.SocketConsumerParams) *SocketConsumer {
	input := coordinator.GetOrCreateSocketCoordinatorConsumer(id, params)
	return &SocketConsumer{
		ID: id,
		Receiver: input,
	}
}

