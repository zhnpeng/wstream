package stream

import (
	"time"
)

// TODO: refine EventChan and Event

type EventType int

const (
	TupleEvent = iota
	MapEvent
)

type Event = interface {
	Type() EventType
	Time() time.Time
	Keys() []interface{}
	UseKeys(indexes ...interface{}) error
	Get(index interface{}) interface{}
	GetMany(indexes ...interface{}) []interface{}
	Set(index, value interface{}) error
}

type EventChan chan Event

type Emitter struct {
	Channels []EventChan
}

func NewEmitter() *Emitter {
	return &Emitter{Channels: make([]EventChan, 0)}
}

func (e *Emitter) RegisterChan(ch EventChan) {
	e.Channels = append(e.Channels, ch)
}

func (e *Emitter) Emit(item Event) {
	for _, channel := range e.Channels {
		channel <- item
	}
}
