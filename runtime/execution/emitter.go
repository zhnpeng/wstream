package execution

import (
	"github.com/zhnpeng/wstream/types"
)

// Emitter is used to emit item to output outEdges
type Emitter interface {
	Length() int
	Add(ch OutEdge)
	Adds(chs ...OutEdge)
	Emit(item types.Item)
	EmitTo(index int, item types.Item)
	Dispose()
}

type MultiEmitter struct {
	outEdges []OutEdge
}

func NewMultiEmitter() *MultiEmitter {
	return &MultiEmitter{outEdges: make([]OutEdge, 0)}
}

func (e *MultiEmitter) Length() int {
	return len(e.outEdges)
}

func (e *MultiEmitter) Add(ch OutEdge) {
	e.outEdges = append(e.outEdges, ch)
}

func (e *MultiEmitter) Adds(chs ...OutEdge) {
	e.outEdges = append(e.outEdges, chs...)
}

// Emit emit item to all output channels
func (e *MultiEmitter) Emit(item types.Item) {
	for _, channel := range e.outEdges {
		channel <- item.Clone()
	}
}

// EmitTo emit item to one output channel
func (e *MultiEmitter) EmitTo(index int, item types.Item) {
	length := len(e.outEdges)
	if length == 0 {
		return
	}
	e.outEdges[index%length] <- item.Clone()
}

// Dispose close all output channels
func (e *MultiEmitter) Dispose() {
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
	e.output <- item.Clone()
}

// EmitTo emit item to one output channel
func (e *SingleEmitter) EmitTo(index int, item types.Item) {
	e.output <- item.Clone()
}

// Dispose close all output channels
func (e *SingleEmitter) Dispose() {
	close(e.output)
}

type GroupEmitter struct {
	outEdges [][]OutEdge
	length   int
}

func NewGroupEmitter() *GroupEmitter {
	return &GroupEmitter{outEdges: make([][]OutEdge, 0), length: 0}
}

func (e *GroupEmitter) Length() int {
	return e.length
}

func (e *GroupEmitter) Add(ch OutEdge) {
	panic("not supported, should use Adds")
}

// Adds GroupEmitter用于rescale算子，每一组的OutEdges数量要求保持一致，不然EmitTo会panic
func (e *GroupEmitter) Adds(chs ...OutEdge) {
	e.outEdges = append(e.outEdges, chs)
	if len(chs) > e.length {
		e.length = len(chs)
	}
}

// Emit emit item to all output channels
func (e *GroupEmitter) Emit(item types.Item) {
	for _, goes := range e.outEdges {
		for _, ch := range goes {
			ch <- item.Clone()
		}
	}
}

// EmitTo emit item to one output channel
func (e *GroupEmitter) EmitTo(index int, item types.Item) {
	if e.length == 0 {
		return
	}
	for _, egos := range e.outEdges {
		// TODO: may panic, refine later
		egos[index%e.length] <- item.Clone()
	}
}

// Dispose close all output channels
func (e *GroupEmitter) Dispose() {
	for _, egos := range e.outEdges {
		for _, ch := range egos {
			close(ch)
		}
	}
}
