package producer

import "github.com/zhnpeng/wstream/multiplexer"

type Producer interface {
	Write(msg multiplexer.Message)
}
