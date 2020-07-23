package windowing

import (
	"container/list"
	"time"

	"github.com/zhnpeng/wstream/funcintfs"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
	"github.com/zhnpeng/wstream/types"
)

/*
WindowContents represent for a specified window (key, Window)
and hold all records belong to this window
*/
type WindowContents struct {
	keys       []interface{}
	window     windows.Window
	elements   *list.List
	reduceFunc funcintfs.WindowReduce
}

// NewWindowContents if window is a TimeWindow collection's T is window's start ts
// else is the first record's time
func NewWindowContents(window windows.Window, t time.Time, keys []interface{}, reduceFunc funcintfs.WindowReduce) *WindowContents {
	return &WindowContents{
		keys:       keys,
		window:     window,
		elements:   list.New(),
		reduceFunc: reduceFunc,
	}
}

// Keys return window's key in string format
func (c *WindowContents) Keys() []interface{} {
	return c.keys
}

// Len is the len of elements list, size of this WindowContents
func (c *WindowContents) Len() int {
	return c.elements.Len()
}

// Iterator return a iteratable object
func (c *WindowContents) Iterator() *list.Element {
	return c.elements.Front()
}

// Window use to get the window related to this WindowContents
func (c *WindowContents) Window() windows.Window {
	return c.window
}

// PushBack push record back to the elements list
func (c *WindowContents) PushBack(record types.Record) {
	c.elements.PushBack(record)
}

// Append reduce elements if reduceFunc is set else push back to list
func (c *WindowContents) Append(record types.Record) {
	if c.reduceFunc == nil {
		c.PushBack(record)
	} else {
		element := c.elements.Front()
		if element == nil {
			acc := c.reduceFunc.Accmulater(c.Window(), record)
			c.PushBack(acc)
		} else {
			acc := element.Value.(types.Record)
			acc = c.reduceFunc.Reduce(acc, record)
			c.Remove(element)
			c.PushBack(acc)
		}
	}
}

// Remove element from collection
func (c *WindowContents) Remove(e *list.Element) {
	c.elements.Remove(e)
}

// RemoveN remove element and return element's next element
// list will set element's next to nil after Remove(element)
// so we get next element before remove and return to caller
func (c *WindowContents) RemoveN(e *list.Element) (next *list.Element) {
	next = e.Next()
	c.elements.Remove(e)
	return
}

// Dispose all elements in WindowContents
func (c *WindowContents) Dispose() {
	c.elements.Init()
}
