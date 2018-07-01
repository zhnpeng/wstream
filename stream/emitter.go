package stream

//Emitter is used to emit item to output channels
type Emitter struct {
	Channels []ItemChan
}

func NewEmitter() *Emitter {
	return &Emitter{Channels: make([]ItemChan, 0)}
}

func (e *Emitter) RegisterChan(ch ItemChan) {
	e.Channels = append(e.Channels, ch)
}

func (e *Emitter) Emit(item Item) {
	for _, channel := range e.Channels {
		channel <- item
	}
}
