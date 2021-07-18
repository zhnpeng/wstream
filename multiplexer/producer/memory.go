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

func (p *Memory) Link(output multiplexer.MessageQueue) {
	p.mutex.Lock()
	p.output = output
	p.mutex.Unlock()
	p.SetState(StateActive)
}

func (p *Memory) UnLink(output multiplexer.MessageQueue) {
	p.mutex.Lock()
	close(p.output)
	p.output = nil
	p.mutex.Unlock()
	p.SetState(StateInActive)
}

func (p *Memory) onMessage(msg multiplexer.Message) {
	// TODO: recover output to closed channel error?
	if !p.IsActive() {
		// skip locking, if is not active
		return
	}
	p.mutex.Lock()
	output := p.output
	p.mutex.Unlock()
	if output != nil {
		output <- msg
	}
}
