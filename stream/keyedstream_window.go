package stream

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/assigners"
)

// Window allow user custom Window behavior
func (s *KeyedStream) Window(assigner assigners.WindowAssinger) *WindowedStream {
	name := "window"
	graph := s.graph
	newStream := s.ToWindowedStream(name)
	graph.AddStreamEdge(s, newStream)

	return newStream
}

// TimeWindow is tumbling time window
func (s *KeyedStream) TimeWindow(period time.Duration) *WindowedStream {
	// name := "TimedWindow"
	return s.Window(nil).Trigger(nil)
}

func (s *KeyedStream) SlidingTimeWindow(period, every time.Duration) *WindowedStream {
	// name := "SlidingTimeWindow"
	return s.Window(nil).Trigger(nil)
}

// CountWindow is tumbling cout window
func (s *KeyedStream) CountWindow(period int64) *WindowedStream {
	// name := "CountWindow"
	return s.Window(nil).Trigger(nil)
}

func (s *KeyedStream) SlidingCountWindow(period, every int64) *WindowedStream {
	// name := "SlidingCountWindow"
	return s.Window(nil).Trigger(nil)
}
