package evictors

import (
	"github.com/zhnpeng/wstream/runtime/operator/windowing"
)

type CountEvictor struct {
	maxCount     int64
	doEvictAfter bool
}

func NewCountEvictor() *CountEvictor {
	return &CountEvictor{}
}

func (e *CountEvictor) EvictBefore(contents *windowing.WindowContents, size int64) {
	if !e.doEvictAfter {
		e.evict(contents, size)
	}
}

func (e *CountEvictor) EvictAfter(contents *windowing.WindowContents, size int64) {
	if e.doEvictAfter {
		e.evict(contents, size)
	}
}

func (e *CountEvictor) evict(contents *windowing.WindowContents, size int64) {
	var evictedCount int64
	for iter := contents.Iterator(); iter != nil; {
		// RemoveN remove element and return its next
		iter = contents.RemoveN(iter)
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
