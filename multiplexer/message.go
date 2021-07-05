package multiplexer

import "github.com/zhnpeng/wstream/types"

type Message struct {
	Topic        string
	PartitionID  int
	PartitionCnt int
	Value        types.Item
}
