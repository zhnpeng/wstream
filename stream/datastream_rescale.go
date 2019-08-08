package stream

import (
	"github.com/zhnpeng/wstream/functions"
	"github.com/zhnpeng/wstream/runtime/selector"
)

func (s *DataStream) Rescale(parallel int, fn functions.Select) *RescaledStream {
	selector := selector.NewSelector(fn)
	stream := s.toRescaleStream(parallel, selector)
	s.connect(stream)
	return stream
}
