package producer

import (
	"context"
	"sync/atomic"

	"gopkg.in/tomb.v1"

	"github.com/zhnpeng/wstream/multiplexer"
)

type ProducerWithState interface {
	GetState() multiplexer.State
	SetState(multiplexer.State)
}

type Producer interface {
	ProducerWithState
	Write(msg ...multiplexer.Message)
	Produce()
	Wait() error
}

type ProducerState struct {
	state int32
	done  chan struct{}
}

func NewProducerState() *ProducerState {
	return &ProducerState{
		state: int32(multiplexer.StateInitializing),
	}
}

func (s *ProducerState) GetState() multiplexer.State {
	return multiplexer.State(atomic.LoadInt32(&s.state))
}

func (s *ProducerState) SetState(newState multiplexer.State) {
	atomic.SwapInt32(&s.state, int32(newState))
}

func (s *ProducerState) Initialized() bool {
	return atomic.CompareAndSwapInt32(&s.state, int32(multiplexer.StateInitializing), int32(multiplexer.StateInitialed))
}

func (s *ProducerState) IsInitialized() bool {
	return s.GetState() >= multiplexer.StateInitialed
}

func (s *ProducerState) IsActive() bool {
	return s.GetState() == multiplexer.StateActive
}

type BasicProducer struct {
	*tomb.Tomb
	*ProducerState
	messages multiplexer.MessageQueue
}

func NewBasicProducer(size int) *BasicProducer {
	return &BasicProducer{
		Tomb:          &tomb.Tomb{},
		ProducerState: NewProducerState(),
		messages:      make(multiplexer.MessageQueue, size),
	}
}

func (p *BasicProducer) Write(msg multiplexer.Message) {
	p.messages.Enqueue(msg)
}

func (p *BasicProducer) controlLoop(ctx context.Context) {
	defer p.Kill(nil)
	defer p.SetState(multiplexer.StateStopped)
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
			// TODO: add on stop?
			p.Done()
			return
		}
	}
}
