package stream

import (
	"github.com/zhnpeng/wstream/runtime/execution"
	"github.com/zhnpeng/wstream/utils/graph"
)

// LocalTransform transform flow in local mode
func (f *Flow) LocalTransform() {
	if f.transformed {
		return
	}
	f.transform()
	f.transformed = true
}

// Transform stream to executable
func (f *Flow) transform() {
	graph.BFSAll(f.Graph, 0, func(v, w int, c int64) {
		fromNode := f.GetFlowNode(v)
		toNode := f.GetFlowNode(w)
		if fromNode.task == nil {
			fromNode.task = fromNode.Stream.ToTask()
		}
		if toNode.task == nil {
			toNode.task = toNode.Stream.ToTask()
		}
		// 这里为了stream和execution完全解耦，选择在这里判断stream类型
		// 来构建底层执行网络，而不是在stream带上跟execution任何相关信息
		// 但是这样比较割裂，按道理stream应该知道底层网络是怎样构建的
		switch fromNode.Stream.(type) {
		case *KeyedStream, *RescaledStream:
			for _, fromN := range fromNode.task.Nodes {
				var groupEdges []execution.OutEdge
				for _, toN := range toNode.task.Nodes {
					edge := make(execution.Edge)
					toN.AddInEdge(edge.In())
					groupEdges = append(groupEdges, edge.Out())
				}
				fromN.AddOutEdges(groupEdges...)
			}
		default:
			for i, n := range fromNode.task.Nodes {
				edge := make(execution.Edge)
				n.AddOutEdge(edge.Out())
				toNode.task.Nodes[i].AddInEdge(edge.In())
			}
		}
	})
}
