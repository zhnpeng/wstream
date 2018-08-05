package stream

type KeyedStream struct {
	Basic
}

func NewKeyedStream(name string, graph *StreamGraph, parallel int, keys []interface{}) *KeyedStream {
	return &KeyedStream{
		Basic: Basic{
			name:     name,
			graph:    graph,
			parallel: parallel,
			options: map[string]interface{}{
				"keys": keys,
			},
		},
	}
}

func (s *KeyedStream) Type() StreamType {
	return TypeKeyedStream
}

func (s *KeyedStream) SetPartition(parallel int) *KeyedStream {
	s.parallel = parallel
	return s
}
