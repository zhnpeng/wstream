package stream

import (
	"sync"
)

type AllToOneOperator struct {
	BasicOperator
}

func (o *AllToOneOperator) connectOperator(opt Operator) (outputChans []EventChan) {
	// only has one outputChan
	outputChans = append(outputChans, make(EventChan))
	o.OutgoingChans = append(o.OutgoingChans, outputChans)
	o.OutgoingOperators.Operators = append(o.OutgoingOperators.Operators, opt)
	return outputChans
}

func NewAllToOneOperator(graph *StreamGraph, parentOperator Operator) *AllToOneOperator {
	// Create new operator
	newOperator := &AllToOneOperator{
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

func (o *AllToOneOperator) Dispose(index int) {
	o.Close(index)
	if o.closedInputs == len(o.Inputs) {
		// close outputChans only if all inputs channels are closed
		for _, outputChans := range o.OutgoingChans {
			for _, outputChan := range outputChans {
				close(outputChan)
			}
		}
	}
}

func (o *AllToOneOperator) Run(wg *sync.WaitGroup) {
	emitters := make(map[int]*Emitter)
	for index := range o.Inputs {
		// Create an empty Emitter for each Input
		// to make sure the task of the last operator
		// in the stream will execute task.Run
		emitters[index] = NewEmitter()
		for _, outputChans := range o.OutgoingChans {
			for _, output := range outputChans {
				// register output chan to all emitters
				emitters[index].RegisterChan(output)
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
				o.Task.Run(item, emitters[id])
			}
		}(index, input)
	}
}
