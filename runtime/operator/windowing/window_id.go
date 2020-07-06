package windowing

import (
	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
	"github.com/zhnpeng/wstream/utils"
)

type WindowID struct {
	key    utils.KeyID
	window windows.Window
}

func NewWindowID(key utils.KeyID, window windows.Window) WindowID {
	return WindowID{
		key:    key,
		window: window,
	}
}

func (wid WindowID) Window() windows.Window {
	return wid.window
}
