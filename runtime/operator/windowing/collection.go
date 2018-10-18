package windowing

import (
	"container/list"
	"time"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type WindowCollection struct {
	t          time.Time
	k          []interface{}
	elements   *list.List
	reduceFunc functions.ReduceFunc
}

// NewWindowCollection if window is a TimeWindow collection's T is window's start ts
// else is the first record's time
func NewWindowCollection(window windows.Window, t time.Time, k []interface{}, reduceFunc functions.ReduceFunc) *WindowCollection {
	return &WindowCollection{
		t:          window.Start(),
		k:          k,
		elements:   list.New(),
		reduceFunc: reduceFunc,
	}
}

func (c *WindowCollection) Keys() []interface{} {
	return c.k
}

// Time return window collection time
// for a GlobalWindow return collection's first element's time
func (c *WindowCollection) Time() time.Time {
	if !c.t.Equal(time.Time{}) {
		return c.t
	}
	t := c.t
	first := c.elements.Front()
	if first != nil {
		t = first.Value.(types.Item).Time()
	}
	return t
}

func (c *WindowCollection) Len() int {
	return c.elements.Len()
}

func (c *WindowCollection) Iterator() *list.Element {
	return c.elements.Front()
}

func (c *WindowCollection) PushBack(record types.Record) {
	c.elements.PushBack(record)
}

// Append reduce elements if reduceFunc is set else push back to list
func (c *WindowCollection) Append(record types.Record) {
	if c.reduceFunc == nil {
		c.PushBack(record)
	} else {
		var acc types.Record
		element := c.elements.Front()
		if element == nil {
			acc = record
			c.PushBack(acc)
		} else {
			acc = element.Value.(types.Record)
			acc = c.reduceFunc.Reduce(acc, record)
			c.Remove(element)
			c.PushBack(acc)
		}
	}
}

// Remove element from collection
func (c *WindowCollection) Remove(e *list.Element) {
	c.elements.Remove(e)
}

// RemoveN remove element and return element's next element
// list will set element's next to nil after Remove(element)
// so we get next element before remove and return to caller
func (c *WindowCollection) RemoveN(e *list.Element) (next *list.Element) {
	next = e.Next()
	c.elements.Remove(e)
	return
}

func (c *WindowCollection) Dispose() {
	c.elements.Init()
}
