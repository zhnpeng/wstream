package task

type NodeType int

const (
	SourceNode NodeType = iota
	BroadcastNode
	RoundRobinNode
)
