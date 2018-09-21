package stream

/*
Punctuated Watermarks and Periodic Watermarks
*/

func (s *DataStream) AssignWatermark() *DataStream {
	name := "assignTimestamp"
	graph := s.graph
	newStream := s.Copy(name)
	graph.AddStreamEdge(s, newStream)

	// newStream.udf = &functions.Map{
	// 	Function: mapFunc,
	// }
	return newStream
}
