package triggers

import "github.com/wandouz/wstream/types"

type Trigger interface {
	OnItem(item types.Item)
	OnProcessingTime()
	OnEventTime()
	CanMerge() bool
	OnMerge()
	Dispose()
}
