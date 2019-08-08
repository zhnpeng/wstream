package intfs

import "github.com/zhnpeng/wstream/types"

type Iterator interface {
	Next() <-chan types.Item
}
