package stream

import (
	"fmt"
	"sync"
)

// TODO: refine EventChan and Event
type Event = interface{}

type EventChan chan Event

type Emitter struct {
	Channels []EventChan
}

func NewEmitter() *Emitter {
	return &Emitter{Channels: make([]EventChan, 0)}
}

func (e *Emitter) RegisterChan(ch EventChan) {
	e.Channels = append(e.Channels, ch)
}

func (e *Emitter) Emit(item Event) {
	for _, channel := range e.Channels {
		channel <- item
	}
}

type Task interface {
	Run(item Event, emitter *Emitter, wg *sync.WaitGroup)
}

type Operator interface {
	Run(wg *sync.WaitGroup)
	AddInputs(inputs ...EventChan)
	//connectOperator is called by parent operator to connect down stream operator
	//this interface should implemted by a terminal Operator such as OneToOneOperator
	//because output channels is decided by parent terminal Operator instead of new Operator
	connectOperator(opt Operator) (outputChans []EventChan)
	SetTask(t Task)
}

type BasicOperator struct {
	Inputs            []EventChan // Is's kind of shards
	OutgoingChans     [][]EventChan
	OutgoingOperators *OperatorSet // Outgoing is for building flow topology
	Task              Task
}

func (b *BasicOperator) AddInputs(inputs ...EventChan) {
	b.Inputs = append(b.Inputs, inputs...)
}

func (b *BasicOperator) SetTask(t Task) {
	b.Task = t
}

type OneToOneOperator struct {
	BasicOperator
}

func (o *OneToOneOperator) connectOperator(opt Operator) (outputChans []EventChan) {
	for range o.Inputs {
		outputChans = append(outputChans, make(EventChan))
	}
	o.OutgoingChans = append(o.OutgoingChans, outputChans)
	o.OutgoingOperators.Operators = append(o.OutgoingOperators.Operators, opt)
	return outputChans
}

func NewOneToOneOperator(graph *StreamGraph, parentOperator Operator) *OneToOneOperator {
	// Create new operator
	newOperator := &OneToOneOperator{
		BasicOperator: BasicOperator{
			OutgoingChans: make([][]EventChan, 0),
		},
	}

	// connect upstream operator to new down stream operator
	parentOutputChans := parentOperator.connectOperator(newOperator)
	// add upstream output Chans to new operator as input Chans
	newOperator.Inputs = parentOutputChans

	// Add out edge into new operator
	newOperator.OutgoingOperators = &OperatorSet{
		Operators: make([]Operator, 0),
		Parent:    newOperator,
	}

	// Add new operator to graph
	graph.Vertexes = append(graph.Vertexes, newOperator.OutgoingOperators)

	return newOperator
}

func (o *OneToOneOperator) Dispose(index int) {
	for _, outputChans := range o.OutgoingChans {
		if len(outputChans) > index {
			close(outputChans[index])
		}
	}
}

func (o *OneToOneOperator) Run(wg *sync.WaitGroup) {
	emitters := make(map[int]*Emitter)
	for index := range o.Inputs {
		// Create an empty Emitter for each Input
		// to make sure the task of the last operator
		// in the stream will execute task.Run
		emitters[index] = NewEmitter()
	}
	for _, outputChans := range o.OutgoingChans {
		for j, output := range outputChans {
			if c, ok := emitters[j]; ok {
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
					o.Dispose(index)
					break
				}
				o.Task.Run(item, emitters[index], wg)
			}
		}()
	}
}

type (
	OperatorFunc func(inputs []EventChan, outputs []EventChan)
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
func (d *DataStream) AddSource(inputs... EventChan) {
	d.Operator.AddInputs(inputs...)
}

func NewDataStream(wg *sync.WaitGroup, inputs... EventChan) *DataStream {
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

type (
	MapFunc func(In Event) (Out Event)
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
	Function func(in Event) Event
}

func (t *MapTask) Run(item Event, out *Emitter, wg *sync.WaitGroup) {
	v := t.Function(item)
	out.Emit(v)
}

func (s *DataStream) Map(mapFunc MapFunc) *DataStream {
	ret := s.Copy()
	parentOperator := s.Operator

	// New Operator
	operator := NewOneToOneOperator(ret.Graph, parentOperator)
	ret.Operator = operator

	// New Task
	task := &MapTask{}
	task.Function = func(in Event) Event {
		return mapFunc(in)
	}
	operator.Task = task
	return ret
}

type PrintfTask struct {
	Function func(i Event) Event
}

func (t *PrintfTask) Run(item Event, out *Emitter, wg *sync.WaitGroup) {
	v := t.Function(item)
	out.Emit(v)
}

func (s *DataStream) Printf(format string) *DataStream {
	ret := s.Copy()
	parentOperator := s.Operator
	operator := NewOneToOneOperator(ret.Graph, parentOperator)
	ret.Operator = operator

	task := &PrintfTask{}
	task.Function = func(i Event) Event {
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
