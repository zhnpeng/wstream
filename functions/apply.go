package functions

import (
	"container/list"
)

type ApplyFunc interface {
	Apply(records *list.Element, emitter Emitter)
}
