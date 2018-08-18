package task

type NodeType int

const (
	TypeSourceNode NodeType = iota
	TypeBroadcastNode
	TypeRoundRobinNode
)

const (
	DefaultWatermarkChannelBufferSize = 300
)
