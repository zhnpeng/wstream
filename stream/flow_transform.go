package stream

import (
	"github.com/sirupsen/logrus"
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
			fromNode.task = f.StreamToTask(fromNode.Stream)
		}
		if toNode.task == nil {
			toNode.task = f.StreamToTask(toNode.Stream)
		}
		// TODO: 这是临时代码，使用更好的方式构建网络
		if _, ok := fromNode.Stream.(*KeyedStream); ok {
			for _, fromN := range fromNode.task.Nodes {
				var groupEdges []execution.OutEdge
				for _, toN := range toNode.task.Nodes {
					edge := make(execution.Edge)
					toN.AddInEdge(edge.In())
					groupEdges = append(groupEdges, edge.Out())
				}
				fromN.AddOutEdges(groupEdges...)
			}
		} else {
			for i, n := range fromNode.task.Nodes {
				edge := make(execution.Edge)
				n.AddOutEdge(edge.Out())
				toNode.task.Nodes[i].AddInEdge(edge.In())
			}
		}
	})
}

func (f *Flow) StreamToTask(stm Stream) *execution.Task {
	switch vstm := stm.(type) {
	case *KeyedStream:
		return vstm.toTask()
	case *DataStream:
		return vstm.toTask()
	case *WindowedStream:
		return vstm.toTask()
	case *SourceStream:
		return vstm.toTask()
	default:
		logrus.Errorf("got unexpected stream: %+v", stm)
	}
	return nil
}
