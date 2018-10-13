package functions

import (
	"container/list"
)

type Iterator interface {
	Next() *list.Element
	Prev() *list.Element
}

type ApplyFunc interface {
	Apply(records Iterator, emitter Emitter)
}
