package stream

// TODO: refine EventChan and Event
type Event = interface{}

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