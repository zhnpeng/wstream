package sio

import (
	"github.com/pkg/errors"
	"github.com/wandouz/wstream/types"
)

//Emitter is used to emit item to output chans
type Emitter struct {
	chans []types.ItemChan
}

func NewEmitter() *Emitter {
	return &Emitter{chans: make([]types.ItemChan, 0)}
}

func (e *Emitter) Length() int {
	return len(e.chans)
}

func (e *Emitter) AddOutput(ch types.ItemChan) {
	e.chans = append(e.chans, ch)
}

// Emit emit item to all output channels
func (e *Emitter) Emit(item types.Item) error {
	for _, channel := range e.chans {
		channel <- item
	}
	return nil
}

// EmitTo emit item to one output channel
func (e *Emitter) EmitTo(item types.Item, index int) error {
	length := len(e.chans)
	if length == 0 {
		return errors.Errorf("no avaliable channel")
	}
	e.chans[index%length] <- item
	return nil
}

// Despose close all output channels
func (e *Emitter) Despose() {
	for _, ch := range e.chans {
		close(ch)
	}
}
