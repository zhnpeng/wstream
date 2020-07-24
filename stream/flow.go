package stream

import (
	"sync"

	"github.com/zhnpeng/wstream/runtime/execution"
	"github.com/zhnpeng/wstream/utils/graph"
)

/*
Flow is a DAG Graph organized with streams
*/
type Flow struct {
	Name        string
	Transformed bool
	Vertices    map[int]*FlowNode
	Graph       *graph.Mutable
}

func NewFlow(name string) *Flow {
	return &Flow{
		Name:     name,
		Vertices: make(map[int]*FlowNode),
		Graph:    graph.New(0),
	}
}

// FlowNode assign unique for each stream
// and combine stream with execution task
type FlowNode struct {
	ID     int
	Stream Stream
	task   *execution.Task
}

func newFlowNode(id int, s Stream) *FlowNode {
	return &FlowNode{
		ID:     id,
		Stream: s,
	}
}

func (f *Flow) GetStream(id int) (stm Stream) {
	if FlowNode, ok := f.Vertices[id]; ok {
		stm = FlowNode.Stream
	}
	return
}

func (f *Flow) GetTask(id int) (task *execution.Task) {
	if t, ok := f.Vertices[id]; ok {
		task = t.task
	}
	return
}

func (f *Flow) Len() int {
	return len(f.Vertices)
}

// AddStream add a stream vertex to Graph
func (f *Flow) AddStream(stm Stream) {
	id := f.Graph.AddVertex()
	flowNode := newFlowNode(id, stm)
	stm.SetFlowNode(flowNode)
	f.Vertices[id] = flowNode
}

// AddStreamEdge add directed edge between two stream
func (f *Flow) AddStreamEdge(from, to Stream) error {
	if !f.existsStream(from) {
		f.AddStream(from)
	}
	if !f.existsStream(to) {
		f.AddStream(to)
	}
	fromID := from.GetFlowNode().ID
	toID := to.GetFlowNode().ID
	return f.Graph.AddEdge(fromID, toID)
}

// CombineStream join right strem to the left
// right stream will not add to Graph
// any FlowNode connect to right stream will collect to the left
func (f *Flow) CombineStream(left, right Stream) {
	if !f.existsStream(left) {
		f.AddStream(left)
	}
	right.SetFlowNode(left.GetFlowNode())
}

func (f *Flow) existsStream(stm Stream) bool {
	FlowNode := stm.GetFlowNode()
	if FlowNode == nil {
		return false
	}
	if _, ok := f.Vertices[FlowNode.ID]; ok {
		return true
	}
	return false
}

func (f *Flow) GetFlowNode(id int) (FlowNode *FlowNode) {
	return f.Vertices[id]
}

func (f *Flow) BFSBoth(v int, do func(v, w int, c int64)) {
	graph.BFSAll(f.Graph, v, do)
}

// Run in local mode
func (f *Flow) Run() {
	f.LocalTransform()
	var wg sync.WaitGroup
	start := f.GetFlowNode(0)
	wg.Add(1)
	go func() {
		defer wg.Done()
		start.task.Run()
	}()
	graph.BFSBoth(f.Graph, 0, func(v, w int, c int64) {
		task := f.GetFlowNode(w).task
		wg.Add(1)
		go func() {
			defer wg.Done()
			task.Run()
		}()
	})
	wg.Wait()
}

// RunDistrib run in distribution mode
// TODO: implement this
func (f *Flow) RunDistrib() {

}
