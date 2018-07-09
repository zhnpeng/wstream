package stream

type Flow struct {
}

type XStreamGraph struct {
	id       int
	Vertexes map[int]*StreamNode
}

func NewXStreamGraph() *XStreamGraph {
	return &XStreamGraph{0, make(map[int]*StreamNode)}
}

//OutputSelector 有两种类型
//broadcast	表示广播到所有输出节点
//redistributing 表示重新分布
//比如keyby按照key hash重新分配
type OutputSelector interface {
	Output(inEdges, outEdges []*StreamNode)
}

type BroadcastOutputSelector struct {
}

func (piper *BroadcastOperator) Output(inEdges, outEdges []*StreamNode) {
	//TODO read from inedges then output to outedges
}

type StreamNode struct {
	ID       int
	Operator Operator
	InEdges  []*StreamNode
	OutEdges []*StreamNode
	Selector OutputSelector
}
