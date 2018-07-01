package stream

type Task interface {
	Run(item Item, emitter *Emitter)
}