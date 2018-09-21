package execution

type Operator interface {
	Run(in *Receiver, out *Emitter)
}
