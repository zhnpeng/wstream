package stream

import (
	"sync"
)

type id = int

type (
	OperatorFunc func(inputs []chan interface{}, outputs []chan interface{})
)

type OperatorGroup struct {
	Operators []*Operator
	Parent    *Operator
}

type OperatorGroupSlice struct {
	Groups []*OperatorGroup
}

type Stream struct {
	OperatorGroups *OperatorGroupSlice
	Operator      *Operator
	WaitGroup     *sync.WaitGroup
}

func (s *Stream) Run() {
	for _, group := range s.OperatorGroups.Groups {
		for _, operator := range group.Operators {
			operator.Task.Run(operator.Inputs, operator.OutgoingChans, s.WaitGroup)
		}
	}
	s.WaitGroup.Wait()
}

type Operator struct {
	Inputs        []chan interface{}
	OutgoingChans [][]chan interface{}
	Task          *Task
	// Outgoing is for building flow structure
	Outgoing *OperatorGroup
}

func NewOperator(inputs []chan interface{}) *Operator {
	ret := &Operator{
		Inputs:        inputs,
		OutgoingChans: make([][]chan interface{}, 0),
	}
	return ret
}

func NewOneToOneOperator(parentOperator *Operator) *Operator {
	/*
	inChan0 <---> outChan0
	inChan1 <---> outChan1
	inChan* <---> outChan*
	*/
	parentOutputs := make([]chan interface{}, 0)
	for i := 0; i < len(parentOperator.Inputs); i++ {
		parentOutputs = append(parentOutputs, make(chan interface{}))
	}
	parentOperator.OutgoingChans = append(
		parentOperator.OutgoingChans,
		parentOutputs,
	)
	ret := NewOperator(parentOutputs)
	return ret
}


type Task struct {
	Operator *Operator
	Function func(in interface{}) interface{}
}

func (t *Task) Run(inputs []chan interface{}, outgoingChans [][]chan interface{}, wg *sync.WaitGroup) {
	for index, input := range inputs {
		wg.Add(1)
		go func(id int, in chan interface{}, wg *sync.WaitGroup) {
			defer wg.Done()
			for {
				i, ok := <-input
				if !ok {
					for _, chans := range outgoingChans {
						close(chans[id])
					}
					return
				}
				ov := t.Function(i)
				for _, chans := range outgoingChans {
					chans[id] <- ov
				}
			}
		}(index, input, wg)
	}
}
