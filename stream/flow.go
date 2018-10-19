package stream

import (
	"sync"

	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/utils/graph"
)

type Stream interface {
	Parallelism() int
	Operator() intfs.Operator
	SetStreamNode(node *StreamNode)
	GetStreamNode() (node *StreamNode)
}

/*
Flow is a DAG graph organized with streams
*/
type Flow struct {
	name     string
	vertices map[int]*StreamNode
	graph    *graph.Mutable
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

func (g *Flow) GetStream(id int) (stm Stream) {
	if StreamNode, ok := g.vertices[id]; ok {
		stm = StreamNode.stream
	}
	return
}

// Length return numbers of vertices of graph
func (g *Flow) Length() int {
	return len(g.vertices)
}

// AddStream add a stream vertex to graph
func (g *Flow) AddStream(stm Stream) {
	id := g.graph.AddVertex()
	StreamNode := newStreamNode(id, stm)
	stm.SetStreamNode(StreamNode)
	g.vertices[id] = StreamNode
}

// AddStreamEdge add directed edge between two stream
func (g *Flow) AddStreamEdge(from, to Stream) error {
	if !g.existsStream(from) {
		g.AddStream(from)
	}
	if !g.existsStream(to) {
		g.AddStream(to)
	}
	fromID := from.GetStreamNode().id
	toID := to.GetStreamNode().id
	return g.graph.AddEdge(fromID, toID)
}

// LeftMergeStream join right strem to the left
// right stream will not add to graph
// any StreamNode connect to right stream will collect to the left
func (g *Flow) LeftMergeStream(left, right Stream) {
	if !g.existsStream(left) {
		g.AddStream(left)
	}
	right.SetStreamNode(left.GetStreamNode())
}

func (g *Flow) existsStream(stm Stream) bool {
	StreamNode := stm.GetStreamNode()
	if StreamNode == nil {
		return false
	}
	if _, ok := g.vertices[StreamNode.id]; ok {
		return true
	}
	return false
}

func (g *Flow) GetStreamNode(id int) (StreamNode *StreamNode) {
	return g.vertices[id]
}

func (g *Flow) BFSBoth(v int, do func(v, w int, c int64)) {
	graph.BFSAll(g.graph, v, do)
}

func (g *Flow) Run() {
	var wg sync.WaitGroup
	start := g.GetStreamNode(0)
	wg.Add(1)
	go func() {
		defer wg.Done()
		start.Task.Run()
	}()
	graph.BFSBoth(g.graph, 0, func(v, w int, c int64) {
		task := g.GetStreamNode(w).Task
		wg.Add(1)
		go func() {
			defer wg.Done()
			task.Run()
		}()
	})
	wg.Wait()
}
