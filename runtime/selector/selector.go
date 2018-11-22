package selector

import (
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/types"
)

type Selector struct {
	function functions.Select
}

func NewSelector(fn functions.Select) *Selector {
	return &Selector{
		function: fn,
	}
}

func (s *Selector) New() intfs.Selector {
	return NewSelector(s.function)
}

func (s *Selector) Select(record types.Record, size int) int {
	return s.function.Select(record, size)
}
