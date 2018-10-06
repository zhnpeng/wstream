package basic

import (
	"container/list"
	"time"

	"github.com/wandouz/wstream/types"
)

type WindowCollection struct {
	T        time.Time
	K        []interface{}
	elements *list.List
}

func NewWindowCollection(t time.Time, k []interface{}) *WindowCollection {
	return &WindowCollection{
		T:        t,
		K:        k,
		elements: list.New(),
	}
}

func (c *WindowCollection) Keys() []interface{} {
	return c.K
}

func (c *WindowCollection) Time() time.Time {
	return c.T
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

func (c *WindowCollection) Remove(e *list.Element) {
	c.elements.Remove(e)
}
