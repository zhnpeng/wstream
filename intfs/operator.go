package intfs

type Operator interface {
	Run(in Iterator, out Emitter)
	New() Operator
}
