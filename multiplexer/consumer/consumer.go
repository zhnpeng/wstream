package consumer

import (
	"context"
	"sync/atomic"

	"github.com/zhnpeng/wstream/multiplexer"

	"gopkg.in/tomb.v1"
)

type ConsumerState struct {
	state int32
}

func NewConsumerState() *ConsumerState {
	return &ConsumerState{
		state: int32(multiplexer.StateInitializing),
	}
}

func (s *ConsumerState) GetState() multiplexer.State {
	return multiplexer.State(atomic.LoadInt32(&s.state))
}

func (s *ConsumerState) SetState(newState multiplexer.State) {
	atomic.SwapInt32(&s.state, int32(newState))
}

func (s *ConsumerState) Initialized() bool {
	return atomic.CompareAndSwapInt32(&s.state, int32(multiplexer.StateInitializing), int32(multiplexer.StateInitialed))
}

func (s *ConsumerState) IsInitialized() bool {
	return s.GetState() >= multiplexer.StateInitialed
}

func (s *ConsumerState) IsActive() bool {
	return s.GetState() == multiplexer.StateActive
}

type ConsumerWithState interface {
	GetState() multiplexer.State
	SetState(multiplexer.State)
}

type Consumer interface {
	ConsumerWithState
	Consume()
	Wait() error
}

type BasicConsumer struct {
	*tomb.Tomb
	*ConsumerState
	onStop func()
}

func NewBasicConsumer() *BasicConsumer {
	return &BasicConsumer{
		ConsumerState: NewConsumerState(),
	}
}

func (c *BasicConsumer) GetState() multiplexer.State {
	return multiplexer.State(atomic.LoadInt32(&c.state))
}

func (c *BasicConsumer) SetOnStop(onStop func()) {
	c.onStop = onStop
}

func (c *BasicConsumer) ControlLoop(ctx context.Context) {
	defer c.Kill(nil) // monitor c.Dying() in message loop
	defer c.SetState(multiplexer.StateStopped)
	for {
		select {
		case <-ctx.Done():
			// TODO: need a control channel and some control event?
			if c.onStop != nil {
				c.onStop()
			}
			return
		}
	}
}
