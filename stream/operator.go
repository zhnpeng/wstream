package stream

import (
	"sync"
)

type Operator interface {
	Run(wg *sync.WaitGroup)
	AddInputs(inputs ...EventChan)
	//connectOperator is called by parent operator to connect down stream operator
	//this interface should implemted by a terminal Operator such as OneToOneOperator
	connectOperator(opt Operator) (outputChans []EventChan)
	SetTask(t Task)
}

type OperatorSet struct {
	Operators []Operator
	Parent    Operator
}

type BasicOperator struct {
	// TODO: refine shards, inputs is now kind of shards
	Inputs            []EventChan
	closedInputs 	  int
	OutgoingChans     [][]EventChan
	OutgoingOperators *OperatorSet // Outgoing is for building flow topology
	Task              Task
}

func (b* BasicOperator) Close(id int) {
	b.closedInputs ++
}

func (b *BasicOperator) AddInputs(inputs ...EventChan) {
	b.Inputs = append(b.Inputs, inputs...)
}

func (b *BasicOperator) SetTask(t Task) {
	b.Task = t
}

