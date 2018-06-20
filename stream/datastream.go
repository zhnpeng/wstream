package stream

import (
	"fmt"
	"sync"
)

type Collector struct {
	Channels []chan interface{}
}

func (c *Collector) RegisterChan(ch chan interface{}) {
	c.Channels = append(c.Channels, ch)
}

func (c *Collector) Emit(item interface{}) {
	for _, channel := range c.Channels {
		channel <- item
	}
}

type Task interface {
	Run(item interface{}, collector Collector, wg *sync.WaitGroup)
}

type Operator interface {
	Run(wg *sync.WaitGroup)
	GetInputs() []chan interface{}
	GetOutgoingChans() [][]chan interface{}
	GetOutgoingOperators() *OperatorSet
}

type BasicOperator struct {
	Inputs            []chan interface{}
	OutgoingChans     [][]chan interface{}
	OutgoingOperators *OperatorSet // Outgoing is for building flow topology
}

func (b *BasicOperator) GetOutgoingOperators() *OperatorSet {
	return b.OutgoingOperators
}

func (b *BasicOperator) GetInputs() []chan interface{} {
	return b.Inputs
}

func (b *BasicOperator) GetOutgoingChans() [][]chan interface{} {
	return b.OutgoingChans
}

type OneToOneOperator struct {
	BasicOperator
	Task Task
}

func NewOneToOneOperator(graph *StreamGraph, parentOperator Operator) *OneToOneOperator {
	// Setup Output channels
	parentOutputs := make([]chan interface{}, 0)
	for i := 0; i < len(parentOperator.GetInputs()); i++ {
		// make the same num of channel as parentOperator's input channels
		parentOutputs = append(parentOutputs, make(chan interface{}))
	}
	outgoingChans := parentOperator.GetOutgoingChans()
	outgoingChans = append(
		outgoingChans,
		parentOutputs,
	)

	// Create new operator
	newOperator := &OneToOneOperator{
		BasicOperator: BasicOperator{
			Inputs:        parentOutputs,
			OutgoingChans: make([][]chan interface{}, 0),
		},
	}
	// Add out edge into new operator
	newOperator.OutgoingOperators = &OperatorSet{
		Operators: make([]Operator, 0),
		Parent:    newOperator,
	}

	// link new operator its parent
	outgoingOperators := parentOperator.GetOutgoingOperators()
	outgoingOperators.Operators = append(
		outgoingOperators.Operators,
		newOperator,
	)

	// Add new operator to graph
	graph.Vertexes = append(graph.Vertexes, newOperator.OutgoingOperators)

	return newOperator
}

func (o *OneToOneOperator) Dispose(index int) {
	for _, outputChans := range o.OutgoingChans {
		close(outputChans[index])
	}
}

func (o *OneToOneOperator) Run(wg *sync.WaitGroup) {
	collectors := make(map[int]Collector)
	for _, outputChans := range o.OutgoingChans {
		for j, output := range outputChans {
			if c, ok := collectors[j]; !ok {
				collector := Collector{Channels: []chan interface{}{output}}
				collectors[j] = collector
			} else {
				c.RegisterChan(output)
			}
		}
	}
	for index, input := range o.Inputs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				item, ok := <-input
				if !ok {
					// dispose output chans
					for _, outputChans := range o.OutgoingChans {
						o.Dispose(index)
					}
					break
				}
				for _, outputChans := range o.OutgoingChans {
					o.Task.Run(item, collectors[index], wg)
				}
			}
		}()
	}
}

type (
	OperatorFunc func(inputs []chan interface{}, outputs []chan interface{})
)

type OperatorSet struct {
	Operators []Operator
	Parent    Operator
}

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
	for _, group := range s.Graph.Vertexes {
		for _, operator := range group.Operators {
			operator.Run(s.WaitGroup)
		}
	}
	s.WaitGroup.Wait()
}

type DataStream struct {
	Stream
}

type (
	MapFunc func(In interface{}) (Out interface{})
)

func (s *DataStream) Copy() *DataStream {
	return &DataStream{
		Stream{
			Graph:     s.Graph,
			Operator:  s.Operator,
			WaitGroup: s.WaitGroup,
		},
	}
}

type MapTask struct {
	Function func(in interface{}) interface{}
}

func (t *MapTask) Run(item interface{}, collector Collector, wg *sync.WaitGroup) {
	v := t.Function(item)
	collector.Emit(v)
}

func (s *DataStream) Map(mapFunc MapFunc) *DataStream {
	ret := s.Copy()
	parentOperator := s.Operator

	// New Operator
	operator := NewOneToOneOperator(ret.Graph, parentOperator)
	ret.Operator = operator

	// New Task
	task := &MapTask{}
	task.Function = func(in interface{}) interface{} {
		return mapFunc(in)
	}
	operator.Task = task
	return ret
}

type PrintfTask struct {
	Function func(i interface{}) interface{}
}

func (t *PrintfTask) Run(item interface{}, collector Collector, wg *sync.WaitGroup) {
	t.Function(item)
}

func (s *DataStream) Printf(format string) *DataStream {
	ret := s.Copy()
	parentOperator := s.Operator
	operator := NewOneToOneOperator(ret.Graph, parentOperator)
	ret.Operator = operator

	task := &PrintfTask{}
	task.Function = func(i interface{}) interface{} {
		fmt.Printf(format+"%+v\n", i)
		return i
	}
	operator.Task = task
	return ret
}

func (s *DataStream) FlatMap() *DataStream {
	return nil
}

func (s *DataStream) Filter() *DataStream {
	return nil
}

func (s *DataStream) KeyBy() *KeyedStream {
	return nil
}
