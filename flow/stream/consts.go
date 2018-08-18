package stream

type StreamType int

const (
	TypeDataStream StreamType = iota
	TypeKeyedStream
	TypeSourceStream
)
