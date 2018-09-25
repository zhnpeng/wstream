package assigners

import "github.com/wandouz/wstream/types"

type WindowAssinger interface {
	AssignWindows(item types.Item)
	GetTrigger()
	IsEventTime() bool
}
