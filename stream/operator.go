package stream

import (
	"sync"
)

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
		go func(id int, inChan EventChan) {
			defer wg.Done()
			for {
				item, ok := <-inChan
				if !ok {
					// dispose output chans
					o.Dispose(id)
					break
				}
				o.Task.Run(item, emitters[id], wg)
			}
		}(index, input)
	}
}
