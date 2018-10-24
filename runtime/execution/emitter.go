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
func (e *Emitter) Emit(item types.Item) {
	for _, channel := range e.outEdges {
		channel <- item
	}
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

// Dispose close all output channels
func (e *Emitter) Dispose() {
	for _, ch := range e.outEdges {
		close(ch)
	}
}

type SingleEmitter struct {
	output OutEdge
}

func NewSingleEmitter(output OutEdge) *SingleEmitter {
	return &SingleEmitter{output}
}

func (e *SingleEmitter) Length() int {
	return 1
}

// Emit emit item to all output channels
func (e *SingleEmitter) Emit(item types.Item) {
	e.output <- item
}

// EmitTo emit item to one output channel
func (e *SingleEmitter) EmitTo(index int, item types.Item) error {
	e.output <- item
	return nil
}

// Dispose close all output channels
func (e *SingleEmitter) Dispose() {
	close(e.output)
}
