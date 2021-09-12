package producer

import (
	"context"
	"sync"

	"github.com/zhnpeng/wstream/multiplexer"
)

type Memory struct {
	*BasicProducer
	output multiplexer.MessageQueue
	mutex  *sync.Mutex
}

func (p *Memory) Produce(ctx context.Context) {
	p.Initialized()
	go p.controlLoop(ctx)
	p.messageLoop(p.onMessage)
}

func (p *Memory) Connect(output multiplexer.MessageQueue) {
	p.mutex.Lock()
	p.output = output
	p.mutex.Unlock()
	p.SetState(multiplexer.StateActive)
}

func (p *Memory) Disconnect() {
	p.mutex.Lock()
	close(p.output)
	p.output = nil
	p.SetState(multiplexer.StateInActive)
}

func (p *Memory) onMessage(msg multiplexer.Message) {
	if output := p.tryConnect(); output != nil {
		output <- msg
	}
}

func (p *Memory) tryConnect() multiplexer.MessageQueue {
	if !p.IsActive() {
		// skip locking, if is not active
		return nil
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.output
}
