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
	var evictedCount int64
	for iter := coll.Iterator(); iter != nil; {
		// RemoveN remove element and return its next
		iter = coll.RemoveN(iter)
		evictedCount++
		if evictedCount >= e.maxCount {
			break
		}
	}
}

func (e *CountEvictor) Of(count int64, doEvictAfter bool) *CountEvictor {
	e.maxCount = count
	e.doEvictAfter = doEvictAfter
	return e
}
