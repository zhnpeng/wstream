package stream

type Task interface {
	Run(item Event, emitter *Emitter)
}