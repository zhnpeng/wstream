package selector

import (
	"sync/atomic"

	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/types"
)

type RoundRobinSelector struct {
	count int64
}

func NewRoundRobinSelector() *RoundRobinSelector {
	return &RoundRobinSelector{}
}

func (m *RoundRobinSelector) New() intfs.Selector {
	return NewRoundRobinSelector()
}

func (m *RoundRobinSelector) Select(record types.Record, size int) int {
	// TODO: find a better implement
	cnt := atomic.AddInt64(&m.count, 1)
	index := cnt % int64(size)
	return int(index)
}
