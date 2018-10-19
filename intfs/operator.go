package intfs

type Operator interface {
	Run(in Receiver, out Emitter)
	New() Operator
}
