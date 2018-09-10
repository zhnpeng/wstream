package execution

type Task struct {
	rescaleNode    *RescaleNode
	broadcastNodes []*BroadcastNode
}

func NewTask(rn *RescaleNode, bn []*BroadcastNode) *Task {
	return &Task{
		rescaleNode:    rn,
		broadcastNodes: bn,
	}
}
