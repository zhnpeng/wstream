package functions

import (
	"container/list"
)

type Apply interface {
	Apply(records *list.Element, emitter Emitter)
}
