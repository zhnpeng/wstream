package stream

import (
	"sync"

	"github.com/zhnpeng/wstream/runtime/execution"
	"github.com/zhnpeng/wstream/utils/graph"
)

/*
Flow is a DAG graph organized with streams
*/
type Flow struct {
	name        string
	transformed bool
	vertices    map[int]*StreamNode
	graph       *graph.Mutable
}

// New a Flow
func NewFlow(name string) *Flow {
	return &Flow{
		name:     name,
		vertices: make(map[int]*StreamNode),
		graph:    graph.New(0),
	}
}

// StreamNode assign unique for each stream
// and combine stream with execution task
type StreamNode struct {
	id     int
	stream Stream
	Task   *execution.Task
}

func newStreamNode(id int, stm Stream) *StreamNode {
	return &StreamNode{
		id:     id,
		stream: stm,
	}
}

func (f *Flow) GetStream(id int) (stm Stream) {
	if StreamNode, ok := f.vertices[id]; ok {
		stm = StreamNode.stream
	}
	return
}

func (f *Flow) GetTask(id int) (task *execution.Task) {
	if t, ok := f.vertices[id]; ok {
		task = t.Task
	}
	return
}

// Len return numbers of vertices of graph
func (f *Flow) Len() int {
	return len(f.vertices)
}

// AddStream add a stream vertex to graph
func (f *Flow) AddStream(stm Stream) {
	id := f.graph.AddVertex()
	StreamNode := newStreamNode(id, stm)
	stm.SetStreamNode(StreamNode)
	f.vertices[id] = StreamNode
}

// AddStreamEdge add directed edge between two stream
func (f *Flow) AddStreamEdge(from, to Stream) error {
	if !f.existsStream(from) {
		f.AddStream(from)
	}
	if !f.existsStream(to) {
		f.AddStream(to)
	}
	fromID := from.GetStreamNode().id
	toID := to.GetStreamNode().id
	return f.graph.AddEdge(fromID, toID)
}

// LeftMergeStream join right strem to the left
// right stream will not add to graph
// any StreamNode connect to right stream will collect to the left
func (f *Flow) LeftMergeStream(left, right Stream) {
	if !f.existsStream(left) {
		f.AddStream(left)
	}
	right.SetStreamNode(left.GetStreamNode())
}

func (f *Flow) existsStream(stm Stream) bool {
	StreamNode := stm.GetStreamNode()
	if StreamNode == nil {
		return false
	}
	if _, ok := f.vertices[StreamNode.id]; ok {
		return true
	}
	return false
}

func (f *Flow) GetStreamNode(id int) (StreamNode *StreamNode) {
	return f.vertices[id]
}

func (f *Flow) BFSBoth(v int, do func(v, w int, c int64)) {
	graph.BFSAll(f.graph, v, do)
}

func (f *Flow) Run() {
	f.Transform()
	var wg sync.WaitGroup
	start := f.GetStreamNode(0)
	wg.Add(1)
	go func() {
		defer wg.Done()
		start.Task.Run()
	}()
	graph.BFSBoth(f.graph, 0, func(v, w int, c int64) {
		task := f.GetStreamNode(w).Task
		wg.Add(1)
		go func() {
			defer wg.Done()
			task.Run()
		}()
	})
	wg.Wait()
}
