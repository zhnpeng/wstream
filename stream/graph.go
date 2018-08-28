package stream

import (
	"sync"

	"github.com/wandouz/wstream/utils/graph"
)

type StreamGraph struct {
	vertices map[int]*streamNode
	graph    *graph.Mutable
	mu       sync.Mutex
}

func NewStreamGraph() *StreamGraph {
	return &StreamGraph{
		vertices: make(map[int]*streamNode),
		graph:    graph.New(0),
	}
}

// streamNode bind stream with id
type streamNode struct {
	id     int
	stream Stream
}

func newStreamNode(id int, stm Stream) *streamNode {
	return &streamNode{
		id:     id,
		stream: stm,
	}
}

// Length return numbers of vertices of graph
func (g *StreamGraph) Length() int {
	return len(g.vertices)
}

// AddStream add a stream vertex to graph
func (g *StreamGraph) AddStream(stm Stream) {
	id := g.graph.AddVertex()
	node := newStreamNode(id, stm)
	stm.SetStreamNode(node)
	g.vertices[id] = node
}

// AddStreamEdge add directed edge between two stream
func (g *StreamGraph) AddStreamEdge(from, to Stream) error {
	if !g.existsStream(from) {
		g.AddStream(from)
	}
	if !g.existsStream(to) {
		g.AddStream(to)
	}
	fromID := from.GetStreamNode().id
	toID := to.GetStreamNode().id
	return g.addEdge(fromID, toID)
}

func (g *StreamGraph) existsStream(stm Stream) bool {
	node := stm.GetStreamNode()
	if node == nil {
		return false
	}
	if g.getVertex(node.id) == nil {
		return false
	}
	return true
}

func (g *StreamGraph) getVertex(id int) *streamNode {
	if v, ok := g.vertices[id]; ok {
		return v
	}
	return nil
}

func (g *StreamGraph) addEdge(v, w int) error {
	return g.graph.AddEdge(v, w)
}
