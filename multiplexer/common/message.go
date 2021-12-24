package common

import (
	"github.com/zhnpeng/wstream/types"
)

//go:generate msgp -o codec_message.go

type Message struct {
	Data         types.Row
}

type MessageQueue chan Message

func FromMapRecord(mr *types.MapRecord) Message {
	return Message{
		Data: mr.AsRow(),
	}
}