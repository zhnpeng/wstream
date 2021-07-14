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
	p.messageForLoop(ctx, p.onMessage)
}

func (p *Console) onMessage(msg multiplexer.Message) {
	fmt.Printf(p.Format, msg)
}
