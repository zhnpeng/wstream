package stream

import (
	"github.com/zhnpeng/wstream/env"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/evictors"

	"github.com/zhnpeng/wstream/runtime/operator/windowing/triggers"

	"github.com/zhnpeng/wstream/runtime/operator"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/assigners"
)

// Window allow user custom Window behavior
func (s *KeyedStream) Window(assigner assigners.WindowAssinger) *WindowedStream {
	stream := s.toWindowedStream()
	stream.assigner = assigner
	stream.operator = operator.NewWindow(assigner, nil)
	s.connect(stream)
	return stream
}

// TimeWindow is tumbling time window
func (s *KeyedStream) TimeWindow(period int64) *WindowedStream {
	if env.Env().TimeCharacteristic == env.IsEventTime {
		// a flow can handle only processing time or event time
		// so need to hold it in a env env
		return s.Window(assigners.NewTumblingEventTimeWindow(period, 0)).
			Trigger(triggers.NewEventTimeTrigger())
	}
	return s.Window(assigners.NewTumblingProcessingTimeWindow(period, 0)).
		Trigger(triggers.NewProcessingTimeTrigger())
}

func (s *KeyedStream) SlidingTimeWindow(period, every int64) *WindowedStream {
	if env.Env().TimeCharacteristic == env.IsEventTime {
		return s.Window(assigners.NewSlidingEventTimeWindoww(period, every, 0)).
			Trigger(triggers.NewEventTimeTrigger())
	}
	return s.Window(assigners.NewSlidingProcessingTimeWindow(period, every, 0)).
		Trigger(triggers.NewProcessingTimeTrigger())
}

// CountWindow is tumbling cout window
func (s *KeyedStream) CountWindow(period int64) *WindowedStream {
	return s.Window(assigners.NewGlobalWindow()).
		Trigger(triggers.NewCountTrigger().Of(period)).
		Evict(evictors.NewCountEvictor().Of(period, true))
}

func (s *KeyedStream) SlidingCountWindow(period, every int64) *WindowedStream {
	return s.Window(assigners.NewGlobalWindow()).
		Trigger(triggers.NewCountTrigger().Of(period)).
		Evict(evictors.NewCountEvictor().Of(every, true))
}
