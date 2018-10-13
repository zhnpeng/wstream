package evictors

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing"
	"github.com/wandouz/wstream/types"
)

type TimeEvictor struct {
	size         time.Duration
	doEvictAfter bool
}

func NewTimeEvictor() *TimeEvictor {
	return &TimeEvictor{}
}

func (e *TimeEvictor) EvictBefore(coll *windowing.WindowCollection, size time.Duration) {
	if !e.doEvictAfter {
		e.evict(coll, size)
	}
}

func (e *TimeEvictor) EvictAfter(coll *windowing.WindowCollection, size time.Duration) {
	if e.doEvictAfter {
		e.evict(coll, size)
	}
}

func (e *TimeEvictor) evict(coll *windowing.WindowCollection, size time.Duration) {
	// evict require elements in coll has timestamp
	maxTime := e.getMaxTime(coll)
	evictBeforeTime := maxTime.Add(-1 * e.size)
	iterator := coll.Iterator()
	for {
		element := iterator.Next()
		if element == nil {
			break
		}
		if item, ok := element.Value.(types.Item); ok {
			if !item.Time().After(evictBeforeTime) {
				coll.Remove(element)
			}
		}
	}
}

func (e *TimeEvictor) getMaxTime(coll *windowing.WindowCollection) time.Time {
	var maxTime time.Time
	iterator := coll.Iterator()
	for {
		element := iterator.Next()
		if element == nil {
			break
		}
		if item, ok := element.Value.(types.Item); ok {
			if item.Time().After(maxTime) {
				maxTime = item.Time()
			}
		}
	}
	return maxTime
}

func (e *TimeEvictor) DoEvictAfter() *TimeEvictor {
	e.doEvictAfter = true
	return e
}

func (e *TimeEvictor) Of(size time.Duration) *TimeEvictor {
	e.size = size
	return e
}
