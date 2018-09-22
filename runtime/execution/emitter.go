package execution

import (
	"github.com/pkg/errors"
	"github.com/wandouz/wstream/types"
)

type Emitter interface {
	Length() int
	Emit(item types.Item) error
	EmitTo(index int, item types.Item) error
}

//FactEmitter is used to emit item to output outEdges
type FactEmitter struct {
	outEdges []OutEdge
}

func NewFactEmitter() *FactEmitter {
	return &FactEmitter{outEdges: make([]OutEdge, 0)}
}

func (e *FactEmitter) Length() int {
	return len(e.outEdges)
}

func (e *FactEmitter) Add(ch OutEdge) {
	e.outEdges = append(e.outEdges, ch)
}

// Emit emit item to all output channels
func (e *FactEmitter) Emit(item types.Item) error {
	for _, channel := range e.outEdges {
		channel <- item
	}
	return nil
}

// EmitTo emit item to one output channel
func (e *FactEmitter) EmitTo(index int, item types.Item) error {
	length := len(e.outEdges)
	if length == 0 {
		return errors.Errorf("no avaliable channel")
	}
	e.outEdges[index%length] <- item
	return nil
}

// Despose close all output channels
func (e *FactEmitter) Despose() {
	for _, ch := range e.outEdges {
		close(ch)
	}
}
