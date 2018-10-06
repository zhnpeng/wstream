package evictors

import "github.com/wandouz/wstream/runtime/operator/windowing/windows"

type CountEvictor struct {
	maxCount     int64
	doEvictAfter bool
}

func NewCountEvictor(count int64, doEvictAfter bool) *CountEvictor {
	return &CountEvictor{
		maxCount:     count,
		doEvictAfter: doEvictAfter,
	}
}

func (e *CountEvictor) EvictBefore(items Iterable, size int64, window windows.Window, ctx EvictorContext) {
	if !e.doEvictAfter {
		e.evict(items, size, ctx)
	}
}

func (e *CountEvictor) EvictAfter(items Iterable, size int64, window windows.Window, ctx EvictorContext) {
	if e.doEvictAfter {
		e.evict(items, size, ctx)
	}
}

func (e *CountEvictor) evict(items Iterable, size int64, ctx EvictorContext) {
	if size <= e.maxCount {
		return
	}
	var evictedCount int64
	for items.HasNext() {
		items.Next()
		evictedCount++
		if evictedCount > (size - e.maxCount) {
			break
		} else {
			items.Remove()
		}
	}
}
