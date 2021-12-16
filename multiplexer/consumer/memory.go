package consumer
import (
	"github.com/zhnpeng/wstream/multiplexer"
	"github.com/zhnpeng/wstream/multiplexer/coordinator"
)

type MemoryConsumer struct {
	ID string
	Receiver multiplexer.MessageQueue
}

func NewMemoryConsumer(id string) *MemoryConsumer {
	input := coordinator.GetOrCreateMemoryCoordinatorConsumer(id)
	return &MemoryConsumer{
		ID: id,
		Receiver: input,
	}
}
