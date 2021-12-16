package multiplexer

import "github.com/zhnpeng/wstream/types"

//go:generate msgp -o codec_message.go

type Message struct {
	Topic        string
	PartitionID  int
	PartitionCnt int
	Data         types.Row
}

type MessageQueue chan Message
