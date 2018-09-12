package stream

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/utils/graph"
)

func stream2task(stream Stream) (task *execution.Task) {
	// transform stream to executable node
	switch stream.(type) {
	case *KeyedStream:
		stm := stream.(*KeyedStream)
		rescaleNode := execution.NewRescaleNode(
			execution.NewReceiver(),
			execution.NewEmitter(),
			context.Background(),
			stm.keys,
		)
		broadcastNodes := make([]execution.Node, 0, stm.parallel)
		for i := 0; i < stm.parallel; i++ {
			broadcastNode := execution.NewBroadcastNode(
				execution.NewReceiver(),
				execution.NewEmitter(),
				context.Background(),
			)
			edge := make(execution.Edge)
			rescaleNode.AddOutEdge(edge.Out())
			broadcastNode.AddInEdge(edge.In())
			broadcastNodes = append(broadcastNodes, broadcastNode)
		}
		task = &execution.Task{
			RescaleNode:    rescaleNode,
			BroadcastNodes: broadcastNodes,
		}
	case *DataStream:
		stm := stream.(*DataStream)
		broadcastNodes := make([]execution.Node, 0, stm.parallel)
		for i := 0; i < stm.parallel; i++ {
			broadcastNode := execution.NewBroadcastNode(
				execution.NewReceiver(),
				execution.NewEmitter(),
				context.Background(),
			)
			broadcastNodes = append(broadcastNodes, broadcastNode)
		}
		task = &execution.Task{
			RescaleNode:    nil,
			BroadcastNodes: broadcastNodes,
		}
	case *SourceStreamChannels:
		stm := stream.(*SourceStreamChannels)
		rescaleNode := execution.NewRoundRobinNode(
			execution.NewReceiver(),
			execution.NewEmitter(),
			context.Background(),
		)
		broadcastNodes := make([]execution.Node, 0, stm.parallel)
		for i := 0; i < stm.parallel; i++ {
			broadcastNode := execution.NewBroadcastNode(
				execution.NewReceiver(),
				execution.NewEmitter(),
				context.Background(),
			)
			edge := make(execution.Edge)
			rescaleNode.AddOutEdge(edge.Out())
			broadcastNode.AddInEdge(edge.In())
			broadcastNodes = append(broadcastNodes, broadcastNode)
		}
		task = &execution.Task{
			RescaleNode:    rescaleNode,
			BroadcastNodes: broadcastNodes,
		}
	default:
		logrus.Errorf("got unexcepted stream: %+v", stream)
	}
	return
}

// Transform stream to executable
func (g *StreamGraph) Transform() {
	graph.BFSAll(g.graph, 0, func(v, w int, c int64) {
		fromNode := g.GetStreamNode(v)
		toNode := g.GetStreamNode(w)
		if fromNode.Task == nil {
			//Create executable
			fromNode.Task = stream2task(fromNode.stream)
		}
		if toNode.Task == nil {
			//Create executable
			toNode.Task = stream2task(toNode.stream)
		}
		if toNode.Task.RescaleNode == nil {
			// is not a rescale node
			for i, n := range fromNode.Task.BroadcastNodes {
				edge := make(execution.Edge)
				n.AddOutEdge(edge.Out())
				toNode.Task.BroadcastNodes[i].AddInEdge(edge.In())
			}
		} else {
			// is a rescale node
			for _, n := range fromNode.Task.BroadcastNodes {
				edge := make(execution.Edge)
				n.AddOutEdge(edge.Out())
				toNode.Task.RescaleNode.AddInEdge(edge.In())
			}
		}
	})
}
