package execution

import "github.com/wandouz/wstream/types"

type InEdge = <-chan types.Item

type OutEdge = chan<- types.Item

type Edge chan types.Item

func (e Edge) In() InEdge {
	return e
}

func (e Edge) Out() OutEdge {
	return e
}
