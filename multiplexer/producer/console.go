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

func (p *Console) Produce(ctx context.Context) {
	p.Initialized()
	go p.controlLoop(ctx)
	p.messageLoop(p.onMessage)
}

func (p *Console) onMessage(msg multiplexer.Message) {
	fmt.Printf(p.Format, msg)
}
