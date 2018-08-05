package functions

import (
	"github.com/wandouz/wstream/streaming/sio"
	"github.com/wandouz/wstream/types"
)

type UserDefinedFunction interface {
	Run(item types.Item, emitter *sio.Emitter)
	Accmulator() types.Item
}
