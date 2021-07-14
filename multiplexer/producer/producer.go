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

func (p *BasicProducer) messageForLoop(ctx context.Context, onMessage func(msg multiplexer.Message)) {
	defer p.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-p.messages.Dequeue():
			if ok {
				onMessage(msg)
			} else {
				return
			}
		}
	}
}
