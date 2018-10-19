package stream

func New(name string) (*Flow, *SourceStream) {
	flow := NewFlow(name)
	return flow, NewSourceStream(flow)
}
