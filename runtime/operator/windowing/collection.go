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
	if !window.Start().Equal(time.Time{}) {
		// if window is not global window
		t = window.Start()
	}
	return &WindowCollection{
		t:          t,
		k:          k,
		elements:   list.New(),
		reduceFunc: reduceFunc,
	}
}

func (c *WindowCollection) Keys() []interface{} {
	return c.k
}

func (c *WindowCollection) Time() time.Time {
	return c.t
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

func (c *WindowCollection) Remove(e *list.Element) {
	c.elements.Remove(e)
}

func (c *WindowCollection) Dispose() {
	c.elements.Init()
}
