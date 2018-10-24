package intfs

import "github.com/wandouz/wstream/types"

type Iterator interface {
	Next() <-chan types.Item
}
