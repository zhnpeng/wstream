package operator

import (
	"github.com/wandouz/wstream/types"
)

type Handler interface {
	handleRecord(record types.Record, out Emitter)
	handleWatermark(wm *types.Watermark, out Emitter)
}

func consume(in Receiver, out Emitter, handler Handler) {
	for {
		item, ok := <-in.Next()
		if !ok {
			return
		}
		switch item.(type) {
		case types.Record:
			handler.handleRecord(item.(types.Record), out)
		case *types.Watermark:
			handler.handleWatermark(item.(*types.Watermark), out)
		}
	}
}
