package main

import (
	"strconv"

	"github.com/wandouz/wstream/stream"
)

func mapFunc(in interface{}) interface{} {
	return in
}

func mapFunc1(in interface{}) interface{} {
	return "++ " + strconv.Itoa(in.(int))
}

func mapFunc2(in interface{}) interface{} {
	return "-- " + strconv.Itoa(in.(int))
}

func filterFunc(i interface{}) bool {
	if v, ok := i.(int); ok {
		return v != 8
	}
	return true
}

func main() {
	input := make(chan interface{})
	datastream := stream.NewChannelStream(input).
		Map(mapFunc).
		Filter(filterFunc)

	datastream.Map(mapFunc1).Printf("S1: ")
	datastream.Map(mapFunc2).Printf("S2: ")
	go func() {
		for i := 0; i < 10; i++ {
			input <- i
		}
		close(input)
	}()
	datastream.Run()
}
