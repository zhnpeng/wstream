package evictors

import (
	"github.com/wandouz/wstream/runtime/operator/windowing"
)

type CountEvictor struct {
	maxCount     int64
	doEvictAfter bool
}

func NewCountEvictor() *CountEvictor {
	return &CountEvictor{}
}

func (e *CountEvictor) EvictBefore(coll *windowing.WindowCollection, size int64) {
	if !e.doEvictAfter {
		e.evict(coll, size)
	}
}

func (e *CountEvictor) EvictAfter(coll *windowing.WindowCollection, size int64) {
	if e.doEvictAfter {
		e.evict(coll, size)
	}
}

func (e *CountEvictor) evict(coll *windowing.WindowCollection, size int64) {
	if size <= e.maxCount {
		return
	}
	var evictedCount int64
	iterator := coll.Iterator()
	for {
		element := iterator.Next()
		if element == nil {
			break
		}
		evictedCount++
		if evictedCount > (size - e.maxCount) {
			break
		} else {
			coll.Remove(element)
		}
	}
}

func (e *CountEvictor) DoEvictAfter() *CountEvictor {
	e.doEvictAfter = true
	return e
}

func (e *CountEvictor) Of(count int64) *CountEvictor {
	e.maxCount = count
	return e
}
