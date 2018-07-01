package main

import (
	"time"

	"github.com/wandouz/wstream/stream"
)

func flatMapFunc(value stream.Value, out *stream.RecordCollector) {
	i := value.Get(0)
	if i.(int) % 2 == 0 {
		out.Emit(value.Copy())
	}
}

func main() {
	input := make(chan stream.Item)
	datastream := stream.NewChannelStream(input).
		FlatMap(flatMapFunc).Printf("")

	go func() {
		for i := 0; i < 10; i++ {
			v := stream.NewTupleValue(i)
			input <- stream.NewRecord(time.Now(), v)
		}
		close(input)
	}()
	datastream.Run()
}
