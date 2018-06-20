package stream

import (
	"sync"
)

type ChannelSourceTask struct {
	Function func(in Event) Event
}

func (t *ChannelSourceTask) Run(item Event, out *Emitter, wg *sync.WaitGroup) {
	out.Emit(item)
}

func NewChannelStream(input chan interface{}) *DataStream {
	var wg sync.WaitGroup
	ret := NewDataStream(&wg)
	ret.AddSource(input)

	task := &ChannelSourceTask{}
	ret.Operator.SetTask(task)
	return ret
}
