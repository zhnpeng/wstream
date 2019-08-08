package evictors

import (
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing"
	"github.com/zhnpeng/wstream/types"
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

func (e *TimeEvictor) EvictBefore(contents *windowing.WindowContents, size int64) {
	if !e.doEvictAfter {
		e.evict(contents, size)
	}
}

func (e *TimeEvictor) EvictAfter(contents *windowing.WindowContents, size int64) {
	if e.doEvictAfter {
		e.evict(contents, size)
	}
}

func (e *TimeEvictor) evict(contents *windowing.WindowContents, size int64) {
	// evict require elements in contents has timestamp
	maxTime := e.getMaxTime(contents)
	evictBeforeTime := maxTime.Add(-1 * time.Duration(e.size) * time.Millisecond)
	for iter := contents.Iterator(); iter != nil; {
		if item, ok := iter.Value.(types.Item); ok {
			if !item.Time().After(evictBeforeTime) {
				// RemoveN remove element and return its next
				iter = contents.RemoveN(iter)
				continue
			}
		}
		iter = iter.Next()
	}
}

func (e *TimeEvictor) getMaxTime(contents *windowing.WindowContents) time.Time {
	var maxTime time.Time
	for iter := contents.Iterator(); iter != nil; iter = iter.Next() {
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
