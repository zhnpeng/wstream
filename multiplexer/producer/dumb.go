package producer

import (
	"context"

	"github.com/zhnpeng/wstream/multiplexer"
)

type Dumb struct {
	*BasicProducer
}

func (p *Dumb) Produce(ctx context.Context) {
	p.Initialized()
	go p.controlLoop(ctx)
	p.messageLoop(p.onMessage)
}

func (p *Dumb) onMessage(msg multiplexer.Message) {}
