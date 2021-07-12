package producer

import (
	"context"

	"gopkg.in/tomb.v1"

	"github.com/zhnpeng/wstream/multiplexer"
)

type Producer interface {
	Write(msg ...multiplexer.Message)
	Produce()
	Wait() error
}

type BasicProducer struct {
	*tomb.Tomb
	messages multiplexer.MessageQueue
}

func (p *BasicProducer) Write(msg multiplexer.Message) {
	p.messages.Enqueue(msg)
}

func (p *BasicProducer) Produce(ctx context.Context) {
	p.Done()
}
