package stream

func (s *DataStream) AssignTimestamp() *DataStream {
	name := "assignTimestamp"
	graph := s.graph
	newStream := s.Copy(name)
	graph.AddStreamEdge(s, newStream)

	// newStream.udf = &functions.Map{
	// 	Function: mapFunc,
	// }
	return newStream
}
