package evictors

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing"
	"github.com/wandouz/wstream/types"
)

/*
TimeEvictor is evictor base on time
time unit for aprameter size is milliseconds
*/
type TimeEvictor struct {
	size         int64
	doEvictAfter bool
}

func NewTimeEvictor() *TimeEvictor {
	return &TimeEvictor{}
}

func (e *TimeEvictor) EvictBefore(coll *windowing.WindowCollection, size int64) {
	if !e.doEvictAfter {
		e.evict(coll, size)
	}
}

func (e *TimeEvictor) EvictAfter(coll *windowing.WindowCollection, size int64) {
	if e.doEvictAfter {
		e.evict(coll, size)
	}
}

func (e *TimeEvictor) evict(coll *windowing.WindowCollection, size int64) {
	// evict require elements in coll has timestamp
	maxTime := e.getMaxTime(coll)
	evictBeforeTime := maxTime.Add(-1 * time.Duration(e.size) * time.Millisecond)
	for iter := coll.Iterator(); iter != nil; {
		if item, ok := iter.Value.(types.Item); ok {
			if !item.Time().After(evictBeforeTime) {
				// RemoveN remove element and return its next
				iter = coll.RemoveN(iter)
				continue
			}
		}
		iter = iter.Next()
	}
}

func (e *TimeEvictor) getMaxTime(coll *windowing.WindowCollection) time.Time {
	var maxTime time.Time
	for iter := coll.Iterator(); iter != nil; iter = iter.Next() {
		if item, ok := iter.Value.(types.Item); ok {
			if item.Time().After(maxTime) {
				maxTime = item.Time()
			}
		}
	}
	return maxTime
}

func (e *TimeEvictor) Of(size int64, doEvictAfter bool) *TimeEvictor {
	e.size = size
	e.doEvictAfter = doEvictAfter
	return e
}
