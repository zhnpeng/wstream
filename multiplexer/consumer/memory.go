package consumer

import (
	"context"

	"gopkg.in/tomb.v1"

	"github.com/zhnpeng/wstream/multiplexer"
)

type Memory struct {
	*BasicConsumer
	input     multiplexer.MessageQueue // FIXME: accept multiple inputs
	onMessage func(msg multiplexer.Message)
}

func NewMemory(input multiplexer.MessageQueue, onMessage func(msg multiplexer.Message)) *Memory {
	return &Memory{
		BasicConsumer: &BasicConsumer{
			Tomb: &tomb.Tomb{},
		},
		input:     input,
		onMessage: onMessage,
	}
}

func (c *Memory) SetOnMessage(onMessage func(message multiplexer.Message)) {
	c.onMessage = onMessage
	c.SetState(multiplexer.StateActive)
}

func (c *Memory) messageLoop() {
	for c.IsActive() {
		select {
		case msg := <-c.input:
			if c.onMessage != nil {
				c.onMessage(msg)
			}
		case <-c.Dying():
			if c.onStop != nil {
				c.onStop()
			}
			return
		}
	}
}

func (c *Memory) Consume(ctx context.Context) {
	go c.messageLoop()
	c.ControlLoop(ctx)
}
