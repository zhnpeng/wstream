package execution

import "github.com/wandouz/wstream/runtime/utils"

type Operator interface {
	Run(in *Receiver, out utils.Emitter)
}
