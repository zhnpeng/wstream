package stream

import (
	"context"

	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/utils/graph"
)

func stream2executable(stream Stream) (task *execution.Task) {
	// transform stream to executable node
	switch stream.(type) {
	case *KeyedStream:
		stm := stream.(*KeyedStream)
		rescaleNode := execution.NewRescaleNode(
			execution.NewReceiver(),
			execution.NewEmitter(),
			stm.keys,
		)
		broadcastNodes := make([]*execution.BroadcastNode, stm.parallel)
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
		task = execution.NewTask(rescaleNode, broadcastNodes)
	case *DataStream:
		stm := stream.(*DataStream)
		broadcastNodes := make([]*execution.BroadcastNode, stm.parallel)
		for i := 0; i < stm.parallel; i++ {
			broadcastNode := execution.NewBroadcastNode(
				execution.NewReceiver(),
				execution.NewEmitter(),
				context.Background(),
			)
			broadcastNodes = append(broadcastNodes, broadcastNode)
		}
		task = execution.NewTask(nil, broadcastNodes)
	case *SourceStreamChannels:
		stm := stream.(*SourceStreamChannels)
		broadcastNodes := make([]*execution.BroadcastNode, stm.parallel)
		for i := 0; i < stm.parallel; i++ {
			broadcastNode := execution.NewBroadcastNode(
				execution.NewReceiver(),
				execution.NewEmitter(),
				context.Background(),
			)
			broadcastNodes = append(broadcastNodes, broadcastNode)
		}
		task = execution.NewTask(nil, broadcastNodes)
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
			fromNode.Task = stream2executable(fromNode.stream)
		}
		if toNode.Task == nil {
			//Create executable
			toNode.Task = stream2executable(toNode.stream)
		}
	})
}
