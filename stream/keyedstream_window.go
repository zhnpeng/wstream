package stream

import (
	"github.com/wandouz/wstream/env"
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
func (s *KeyedStream) TimeWindow(period int64) *WindowedStream {
	if env.ENV.TimeCharacteristic == env.IsEventTime {
		// a flow can handle only processing time or event time
		// so need to hold it in a env env
		return s.Window(assigners.NewTumblingEventTimeWindow(period, 0)).
			Trigger(triggers.NewEventTimeTrigger())
	} else {
		return s.Window(assigners.NewTumblingProcessingTimeWindow(period, 0)).
			Trigger(triggers.NewProcessingTimeTrigger())
	}
}

func (s *KeyedStream) SlidingTimeWindow(period, every int64) *WindowedStream {
	if env.ENV.TimeCharacteristic == env.IsEventTime {
		return s.Window(assigners.NewSlidingEventTimeWindoww(period, every, 0)).
			Trigger(triggers.NewEventTimeTrigger())
	} else {
		return s.Window(assigners.NewSlidingProcessingTimeWindow(period, every, 0)).
			Trigger(triggers.NewProcessingTimeTrigger())
	}
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
