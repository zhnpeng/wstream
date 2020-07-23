package funcintfs

import (
	"container/list"

	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
)

type Apply interface {
	Apply(window windows.Window, records *list.Element, emitter Emitter)
}
