package stream

import (
	"github.com/sirupsen/logrus"
	"github.com/zhnpeng/wstream/runtime/execution"
	"github.com/zhnpeng/wstream/utils/graph"
)

// LocalTransform transform flow in local mode
func (f *Flow) LocalTransform() {
	f.transform()
}

// Transform stream to executable
func (f *Flow) transform() {
	graph.BFSAll(f.Graph, 0, func(v, w int, c int64) {
		fromNode := f.GetStreamNode(v)
		toNode := f.GetStreamNode(w)
		if fromNode.task == nil {
			//Create executable
			fromNode.task = f.StreamToTask(fromNode.Stream)
		}
		if toNode.task == nil {
			//Create executable
			toNode.task = f.StreamToTask(toNode.Stream)
		}
		if toNode.task.RescaleNode == nil {
			// is a broadcast node
			for i, n := range fromNode.task.BroadcastNodes {
				edge := make(execution.Edge)
				n.AddOutEdge(edge.Out())
				toNode.task.BroadcastNodes[i].AddInEdge(edge.In())
			}
		} else {
			// is a rescale node
			for _, n := range fromNode.task.BroadcastNodes {
				edge := make(execution.Edge)
				n.AddOutEdge(edge.Out())
				toNode.task.RescaleNode.AddInEdge(edge.In())
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
