package execution

import (
	"github.com/pkg/errors"
	"github.com/wandouz/wstream/types"
)

//Emitter is used to emit item to output outEdges
type Emitter struct {
	outEdges []OutEdge
}

func NewEmitter() *Emitter {
	return &Emitter{outEdges: make([]OutEdge, 0)}
}

func (e *Emitter) Length() int {
	return len(e.outEdges)
}

func (e *Emitter) Add(ch OutEdge) {
	e.outEdges = append(e.outEdges, ch)
}

// Emit emit item to all output channels
func (e *Emitter) Emit(item types.Item) error {
	for _, channel := range e.outEdges {
		channel <- item
	}
	return nil
}

// EmitTo emit item to one output channel
func (e *Emitter) EmitTo(index int, item types.Item) error {
	length := len(e.outEdges)
	if length == 0 {
		return errors.Errorf("no avaliable channel")
	}
	e.outEdges[index%length] <- item
	return nil
}

// Despose close all output channels
func (e *Emitter) Despose() {
	for _, ch := range e.outEdges {
		close(ch)
	}
}
