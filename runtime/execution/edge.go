package execution

import (
	"github.com/zhnpeng/wstream/types"
)

type Edge interface {
	In() InEdge
	Out() OutEdge
}

type InEdge = <-chan types.Item

type OutEdge = chan<- types.Item

type LocalEdge struct {
	ch chan types.Item
}

func NewLocalEdge() *LocalEdge {
	return &LocalEdge{
		ch: make(chan types.Item, 0),
	}
}

func (e *LocalEdge) In() InEdge {
	return e.ch
}

func (e *LocalEdge) Out() OutEdge {
	return e.ch
}
