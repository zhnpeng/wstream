package main

import (
	"strconv"
	"time"

	"github.com/wandouz/wstream/stream"
)

func mapFunc(value stream.Value) stream.Value {
	return value
}

func mapFunc1(value stream.Value) stream.Value {
	tmp := "++ " + strconv.Itoa(value.Get(0).(int))
	newV := stream.NewTupleValue(tmp)
	return newV
}

func mapFunc2(value stream.Value) stream.Value {
	tmp := "-- " + strconv.Itoa(value.Get(0).(int))
	newV := stream.NewTupleValue(tmp)
	return newV
}

func filterFunc(value stream.Value) bool {
	if i, ok := value.Get(0).(int); ok {
		return i != 8
	}
	return true
}

func main() {
	input := make(chan stream.Item)
	datastream := stream.NewChannelStream(input).
		Map(mapFunc).
		Filter(filterFunc)

	datastream.Map(mapFunc1).Printf("S1: ")
	datastream.Map(mapFunc2).Printf("S2: ")
	go func() {
		for i := 0; i < 10; i++ {
			v := stream.NewTupleValue(i)
			input <- stream.NewRecord(time.Now(), v)
		}
		close(input)
	}()
	datastream.Run()
}
