package producer

import (
	"context"
	"sync/atomic"

	"gopkg.in/tomb.v1"

	"github.com/zhnpeng/wstream/multiplexer"
)

type State int32

const (
	StateInitializing = State(iota)
	StateInitialed    = State(iota)
	StateInActive     = State(iota)
	// StateActive represent for ready to process data
	StateActive  = State(iota)
	StateStopped = State(iota)
	numStates    = iota
)

type ProducerWithState interface {
	GetState() State
	SetState(State)
}

type Producer interface {
	ProducerWithState
	Write(msg ...multiplexer.Message)
	Produce()
	Wait() error
}

type ProducerState struct {
	state int32
}

func NewProducerState() *ProducerState {
	return &ProducerState{
		state: int32(StateInitializing),
	}
}

func (s *ProducerState) GetState() State {
	return State(atomic.LoadInt32(&s.state))
}

func (s *ProducerState) SetState(newState State) {
	atomic.SwapInt32(&s.state, int32(newState))
}

func (s *ProducerState) Initialized() bool {
	return atomic.CompareAndSwapInt32(&s.state, int32(StateInitializing), int32(StateInitialed))
}

func (s *ProducerState) IsInitialized() bool {
	return s.GetState() >= StateInitialed
}

func (s *ProducerState) IsActive() bool {
	return s.GetState() == StateActive
}

type BasicProducer struct {
	*tomb.Tomb
	*ProducerState
	messages multiplexer.MessageQueue
}

func NewBasicProducer(size int) *BasicProducer {
	return &BasicProducer{
		&tomb.Tomb{},
		NewProducerState(),
		make(multiplexer.MessageQueue, size),
	}
}

func (p *BasicProducer) Write(msg multiplexer.Message) {
	p.messages.Enqueue(msg)
}

func (p *BasicProducer) controlLoop(ctx context.Context) {
	defer p.Done()
	defer p.SetState(StateStopped)
	for {
		select {
		case <-ctx.Done():
			return
		}
	}
}

func (p *BasicProducer) messageLoop(onMessage func(msg multiplexer.Message)) {
	for p.IsInitialized() {
		select {
		case msg, ok := <-p.messages.Dequeue():
			if ok {
				onMessage(msg)
			}
		case <-p.Dying():
			return
		}
	}
}
