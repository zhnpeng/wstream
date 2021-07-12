package producer

import (
	"context"
	"fmt"

	"github.com/zhnpeng/wstream/multiplexer"
)

type Console struct {
	*BasicProducer
	Format string
}

func (p *Console) messageForLoop(ctx context.Context) {
	defer p.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-p.messages.Dequeue():
			if ok {
				p.onMessage(msg)
			} else {
				return
			}
		}
	}
}

func (p *Console) Produce(ctx context.Context) {
	p.messageForLoop(ctx)
}

func (p *Console) onMessage(msg multiplexer.Message) {
	fmt.Printf(p.Format, msg)
}
