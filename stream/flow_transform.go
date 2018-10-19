package stream

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/utils/graph"
)

// Transform stream to executable
func (f *Flow) Transform() {
	graph.BFSAll(f.graph, 0, func(v, w int, c int64) {
		fromNode := f.GetStreamNode(v)
		toNode := f.GetStreamNode(w)
		if fromNode.Task == nil {
			//Create executable
			fromNode.Task = f.StreamToTask(fromNode.stream)
		}
		if toNode.Task == nil {
			//Create executable
			toNode.Task = f.StreamToTask(toNode.stream)
		}
		if len(toNode.Task.RescaleNodes) == 0 {
			// is a broadcast node
			for i, n := range fromNode.Task.BroadcastNodes {
				edge := make(execution.Edge)
				n.AddOutEdge(edge.Out())
				toNode.Task.BroadcastNodes[i].AddInEdge(edge.In())
			}
		} else {
			// is a rescale node
			length := len(toNode.Task.RescaleNodes)
			for i, n := range fromNode.Task.BroadcastNodes {
				edge := make(execution.Edge)
				n.AddOutEdge(edge.Out())
				toNode.Task.RescaleNodes[i%length].AddInEdge(edge.In())
			}
		}
	})
}

func (f *Flow) StreamToTask(stm Stream) *execution.Task {
	switch stm.(type) {
	case *KeyedStream:
		return f.KeyedStreamToTask(stm.(*KeyedStream))
	case *DataStream:
		return f.DataStreamToTask(stm.(*DataStream))
	case *WindowedStream:
		return f.WindowedStreamToTask(stm.(*WindowedStream))
	case *SourceStream:
		return f.SourceStreamToTask(stm.(*SourceStream))
	default:
		logrus.Errorf("got unexpected stream: %+v", stm)
	}
	return nil
}

func (f *Flow) KeyedStreamToTask(stm *KeyedStream) (task *execution.Task) {
	rescaleNode := execution.NewNode(
		context.Background(),
		stm.Operator(),
		execution.NewReceiver(),
		execution.NewEmitter(),
	)
	broadcastNodes := make([]*execution.Node, 0, stm.Parallelism())
	for i := 0; i < stm.Parallelism(); i++ {
		broadcastNode := execution.NewNode(
			context.Background(),
			stm.Operator(),
			execution.NewReceiver(),
			execution.NewEmitter(),
		)
		edge := make(execution.Edge)
		rescaleNode.AddOutEdge(edge.Out())
		broadcastNode.AddInEdge(edge.In())
		broadcastNodes = append(broadcastNodes, broadcastNode)
	}
	task = &execution.Task{
		RescaleNodes:   []*execution.Node{rescaleNode},
		BroadcastNodes: broadcastNodes,
	}
	return
}

func (f *Flow) DataStreamToTask(stm *DataStream) (task *execution.Task) {
	broadcastNodes := make([]*execution.Node, 0, stm.Parallelism())
	for i := 0; i < stm.Parallelism(); i++ {
		node := execution.NewNode(
			context.Background(),
			stm.Operator(),
			execution.NewReceiver(),
			execution.NewEmitter(),
		)
		broadcastNodes = append(broadcastNodes, node)
	}
	task = &execution.Task{
		BroadcastNodes: broadcastNodes,
	}
	return
}

func (f *Flow) WindowedStreamToTask(stm *WindowedStream) (task *execution.Task) {
	broadcastNodes := make([]*execution.Node, 0, stm.Parallelism())
	for i := 0; i < stm.Parallelism(); i++ {
		node := execution.NewNode(
			context.Background(),
			stm.Operator(),
			execution.NewReceiver(),
			execution.NewEmitter(),
		)
		broadcastNodes = append(broadcastNodes, node)
	}
	task = &execution.Task{
		BroadcastNodes: broadcastNodes,
	}
	return
}

func (f *Flow) SourceStreamToTask(stm *SourceStream) (task *execution.Task) {
	rescaleNode := execution.NewNode(
		context.Background(),
		stm.Operator(),
		execution.NewReceiver(),
		execution.NewEmitter(),
	)
	for _, input := range stm.Inputs {
		rescaleNode.AddInEdge(execution.Edge(input).In())
	}
	broadcastNodes := make([]*execution.Node, 0, stm.Parallelism())
	for i := 0; i < stm.Parallelism(); i++ {
		broadcastNode := execution.NewNode(
			context.Background(),
			stm.Operator(),
			execution.NewReceiver(),
			execution.NewEmitter(),
		)
		edge := make(execution.Edge)
		rescaleNode.AddOutEdge(edge.Out())
		broadcastNode.AddInEdge(edge.In())
		broadcastNodes = append(broadcastNodes, broadcastNode)
	}
	task = &execution.Task{
		RescaleNodes:   []*execution.Node{rescaleNode},
		BroadcastNodes: broadcastNodes,
	}
	return
}
