package main

import (
	"fmt"
	"github.com/wandouz/wstream/stream"
	"strconv"
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

func main() {
	input := make(chan interface{})
	datastream := stream.NewChannelStream(input)
	// Map(mapFunc)
	// datastream.Map(mapFunc1).Printf("S1: ")
	// datastream.Map(mapFunc2).Printf("S2: ")
	go func() {
		for i := 0; i < 10; i++ {
			input <- i
		}
		close(input)
	}()
	for _, operator := range datastream.Graph.Vertexes {
		fmt.Println(operator)
	}
	datastream.Run()
}
