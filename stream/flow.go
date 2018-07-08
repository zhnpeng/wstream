package stream

type Flow struct {
}

type NewStreamGraph struct {
	verticesMap map[string]*StreamNode
	vertices    []*StreamNode
}

//OutputSelector has two type round-robin and broadcast
type OutputSelector interface {
}

type StreamNode struct {
	ID             string
	Operator       Operator
	InEdges        []*StreamNode
	OutEdges       []*StreamNode
	OutputSelector OutputSelector
}

// type StreamEdge struct {
// 	SourceNode *StreamNode
// 	TargetNode *StreamNode
// }
