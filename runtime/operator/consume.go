package operator

import (
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

type Handler interface {
	handleRecord(record types.Record, out utils.Emitter)
	handleWatermark(wm *types.Watermark, out utils.Emitter)
}

func consume(in *execution.Receiver, out utils.Emitter, handler Handler) {
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
