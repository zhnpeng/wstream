package stream

import (
	"sync"
)

type ChannelSourceTask struct {
	Function func(in Item) Item
}

func (t *ChannelSourceTask) Run(item Item, out *Emitter) {
	out.Emit(item)
}

func NewChannelStream(input ItemChan) *DataStream {
	var wg sync.WaitGroup
	ret := NewDataStream(&wg)
	ret.AddSource(input)

	task := &ChannelSourceTask{}
	ret.Operator.SetTask(task)
	return ret
}
