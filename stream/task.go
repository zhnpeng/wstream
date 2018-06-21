package stream

import (
	"sync"
)

type Task interface {
	Run(item Event, emitter *Emitter, wg *sync.WaitGroup)
}