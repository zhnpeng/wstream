package stream

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/evictors"

	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"

	"github.com/wandouz/wstream/runtime/operator"
	"github.com/wandouz/wstream/runtime/operator/windowing/assigners"
)

// Window allow user custom Window behavior
func (s *KeyedStream) Window(assigner assigners.WindowAssinger) *WindowedStream {
	name := "window"
	graph := s.graph
	newStream := s.ToWindowedStream(name)
	graph.AddStreamEdge(s, newStream)
	newStream.assigner = assigner
	newStream.operator = operator.NewWindow(assigner, nil)

	return newStream
}

// TimeWindow is tumbling time window
func (s *KeyedStream) TimeWindow(period time.Duration) *WindowedStream {
	return s.Window(nil).Trigger(nil)
}

func (s *KeyedStream) SlidingTimeWindow(period, every time.Duration) *WindowedStream {
	return s.Window(nil).Trigger(nil)
}

// CountWindow is tumbling cout window
func (s *KeyedStream) CountWindow(period int64) *WindowedStream {
	return s.Window(assigners.NewGlobalWindow()).
		Trigger(triggers.NewCountTrigger().Of(period)).
		Evict(evictors.NewCountEvictor().Of(period))
}

func (s *KeyedStream) SlidingCountWindow(period, every int64) *WindowedStream {
	return s.Window(assigners.NewGlobalWindow()).
		Trigger(triggers.NewCountTrigger().Of(period)).
		Evict(evictors.NewCountEvictor().Of(every))
}
