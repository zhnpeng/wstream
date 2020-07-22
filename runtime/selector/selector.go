package selector

import (
	"github.com/zhnpeng/wstream/funcintfs"
	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/types"
)

type Selector struct {
	function funcintfs.Select
}

func NewSelector(fn funcintfs.Select) *Selector {
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
