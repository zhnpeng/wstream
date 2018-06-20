package stream

import (
	"fmt"
	"sync"
)

type ChannelSourceTask struct {
	Function func(in Event) Event
}

func (t *ChannelSourceTask) Run(item Event, out Emitter, wg *sync.WaitGroup) {
	fmt.Println(item)
	out.Emit(item)
}

func NewChannelStream(input chan interface{}) *DataStream {
	var wg sync.WaitGroup
	ret := NewDataStream(&wg)
	ret.AddSources(input)

	task := &ChannelSourceTask{}
	ret.Operator.SetTask(task)
	return ret
}
