package stream

import (
	"sync"
)



type StreamGraph struct {
	// TODO: make it turly DAG, cause it's a tree now.
	Vertexes []*OperatorSet
}

type Stream struct {
	Graph     *StreamGraph
	Operator  Operator
	WaitGroup *sync.WaitGroup
}

func (s *Stream) Run() {
	for _, set := range s.Graph.Vertexes {
		for _, operator := range set.Operators {
			operator.Run(s.WaitGroup)
		}
	}
	s.WaitGroup.Wait()
}

type DataStream struct {
	Stream
}

/*
AddSource : add source to datastream
Because outputchans will be add when adding next operator
So you can add extern source anywhere you want
Example:
ds := NewDataStream(args...)
ds.AddSource(input1)
ds.Map(args1...)
ds.AddSource(input2, input3)
ds.Map(args2...)
*/
func (d *DataStream) AddSource(inputs ...EventChan) {
	d.Operator.AddInputs(inputs...)
}

func NewDataStream(wg *sync.WaitGroup, inputs ...EventChan) *DataStream {
	outputChans := make([]EventChan, 0)
	for range inputs {
		outputChans = append(outputChans, make(EventChan))
	}
	operator := &OneToOneOperator{
		BasicOperator: BasicOperator{
			Inputs:        inputs,
			OutgoingChans: [][]EventChan{outputChans},
		},
	}
	// first operator's outgoing operator set
	initOperatorSet := &OperatorSet{
		Parent:    operator,
		Operators: make([]Operator, 0),
	}
	operator.OutgoingOperators = initOperatorSet
	// head vertex
	headOperatorSet := &OperatorSet{
		Parent:    nil,
		Operators: []Operator{operator},
	}
	return &DataStream{
		Stream{
			Graph: &StreamGraph{
				Vertexes: []*OperatorSet{
					headOperatorSet,
					initOperatorSet,
				},
			},
			Operator:  operator,
			WaitGroup: wg,
		},
	}
}

func (s *DataStream) Copy() *DataStream {
	return &DataStream{
		Stream{
			Graph:     s.Graph,
			Operator:  s.Operator,
			WaitGroup: s.WaitGroup,
		},
	}
}


