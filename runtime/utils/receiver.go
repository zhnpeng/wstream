package utils

import "github.com/wandouz/wstream/types"

type Receiver interface {
	Next() <-chan types.Item
}
