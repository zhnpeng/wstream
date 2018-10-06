package evictors

import (
	"github.com/wandouz/wstream/runtime/operator/windowing/basic"
	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
)

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

func (e *CountEvictor) EvictBefore(coll *basic.WindowCollection, size int64, window windows.Window, ctx EvictorContext) {
	if !e.doEvictAfter {
		e.evict(coll, size, ctx)
	}
}

func (e *CountEvictor) EvictAfter(coll *basic.WindowCollection, size int64, window windows.Window, ctx EvictorContext) {
	if e.doEvictAfter {
		e.evict(coll, size, ctx)
	}
}

func (e *CountEvictor) evict(coll *basic.WindowCollection, size int64, ctx EvictorContext) {
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
