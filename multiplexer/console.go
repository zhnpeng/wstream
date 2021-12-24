package multiplexer

import (
	"fmt"
	"github.com/zhnpeng/wstream/multiplexer/common"
)

type ConsoleProducer struct {
	ID string
	Format string
	Sender common.MessageQueue
}

func NewConsoleProducer(id string, format string) *ConsoleProducer {
	output := make(common.MessageQueue, 100)
	ret := &ConsoleProducer{
		ID:     id,
		Format: format,
		Sender: output,
	}
	go ret.run()
	return ret
}

func (p *ConsoleProducer) run() {
	for {
		select {
			case msg, ok := <- p.Sender:
				if !ok {
					return
				}
				fmt.Printf(p.Format, msg)
		}
	}
}

