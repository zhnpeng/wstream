package multiplexer

type State int32

const (
	StateInitializing = State(iota)
	StateInitialed    = State(iota)
	StateInActive     = State(iota)
	// StateActive represent for ready to process data
	StateActive  = State(iota)
	StateStopped = State(iota)
	numStates    = iota
)
