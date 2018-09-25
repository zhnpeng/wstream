package stream

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/utils/graph"
)

func stream2task(stream Stream) (task *execution.Task) {
	// TODO: refine me, tranformation may implement by each stream operator
	// transform stream to executable node
	switch stream.(type) {
	case *KeyedStream:
		stm := stream.(*KeyedStream)
		rescaleNode := execution.NewNode(
			execution.NewReceiver(),
			execution.NewEmitter(),
			stm.operator,
			context.Background(),
		)
		broadcastNodes := make([]*execution.Node, 0, stm.Parallelism())
		for i := 0; i < stm.Parallelism(); i++ {
			broadcastNode := execution.NewNode(
				execution.NewReceiver(),
				execution.NewEmitter(),
				stm.operator,
				context.Background(),
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
	case *DataStream:
		stm := stream.(*DataStream)
		broadcastNodes := make([]*execution.Node, 0, stm.Parallelism())
		for i := 0; i < stm.Parallelism(); i++ {
			node := execution.NewNode(
				execution.NewReceiver(),
				execution.NewEmitter(),
				stm.operator,
				context.Background(),
			)
			broadcastNodes = append(broadcastNodes, node)
		}
		task = &execution.Task{
			BroadcastNodes: broadcastNodes,
		}
	case *SourceStream:
		stm := stream.(*SourceStream)
		rescaleNode := execution.NewNode(
			execution.NewReceiver(),
			execution.NewEmitter(),
			stm.operator,
			context.Background(),
		)
		for _, input := range stm.Inputs {
			rescaleNode.AddInEdge(execution.Edge(input).In())
		}
		broadcastNodes := make([]*execution.Node, 0, stm.Parallelism())
		for i := 0; i < stm.Parallelism(); i++ {
			broadcastNode := execution.NewNode(
				execution.NewReceiver(),
				execution.NewEmitter(),
				stm.operator,
				context.Background(),
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
		if len(toNode.Task.RescaleNodes) == 0 {
			// is not a rescale node
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
