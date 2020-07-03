package windowing

import (
	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
	"github.com/zhnpeng/wstream/utils"
)

type WindowID struct {
	k utils.KeyID
	w windows.Window
}

func NewWindowID(k utils.KeyID, w windows.Window) WindowID {
	return WindowID{
		k: k,
		w: w,
	}
}

func (wid WindowID) Window() windows.Window {
	return wid.w
}

func (wid WindowID) Key() utils.KeyID {
	return wid.k
}
