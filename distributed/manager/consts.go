package manager

//TODO refine signale should classify request signal and response msg
const (
	UnknownError = "\006"
	OK           = "\007"
	Terminate    = "\011"
	PingSignal   = "\012"
	WorkerReady  = "\013" //  Signals worker is ready
)
