package producer

import (
	"context"

	"github.com/zhnpeng/wstream/multiplexer"
)

type Memory struct {
	*BasicProducer
	output multiplexer.MessageQueue
	//mutex  sync.Mutex
}

func (p *Memory) Produce(ctx context.Context) {
	p.messageForLoop(ctx, p.onMessage)
}

//func (p *Memory) Link(output multiplexer.MessageQueue) {
//	defer p.mutex.Unlock()
//	p.mutex.Lock()
//	p.output = output
//}
//
//func (p *Memory) UnLink() {
//	defer p.mutex.Unlock()
//	p.mutex.Lock()
//	p.output = nil
//}

func (p *Memory) messageForLoop(ctx context.Context, onMessage func(msg multiplexer.Message)) {
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

func (p *Memory) onMessage(msg multiplexer.Message) {
	if p.output != nil {
		p.output <- msg
	}
}
