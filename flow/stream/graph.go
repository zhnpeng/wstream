package stream

import (
	"sync"

	"github.com/pkg/errors"
)

type StreamGraph struct {
	Vertices map[int]*streamNode
	length   int
	mu       sync.Mutex
}

func NewStreamGraph() *StreamGraph {
	return &StreamGraph{
		Vertices: make(map[int]*streamNode),
	}
}

type streamNode struct {
	ID        int
	Value     Stream
	Outgoings map[int]*streamNode
	Incomings map[int]*streamNode
}

func newStreamNode(id int, stm Stream) *streamNode {
	return &streamNode{
		ID:        id,
		Value:     stm,
		Incomings: make(map[int]*streamNode),
		Outgoings: make(map[int]*streamNode),
	}
}

func (g *StreamGraph) Length() int {
	return g.length
}

func (g *StreamGraph) AddStream(stm Stream) error {
	g.mu.Lock()
	id := g.length
	g.length++
	g.mu.Unlock()
	node := newStreamNode(id, stm)
	stm.SetStreamNode(node)
	return g.AddVertex(id, node)
}

func (g *StreamGraph) AddStreamEdge(from, to Stream) error {
	if !g.ExistsStream(from) {
		g.AddStream(from)
	}
	if !g.ExistsStream(to) {
		g.AddStream(to)
	}
	fromID := from.GetStreamNode().ID
	toID := to.GetStreamNode().ID
	return g.AddEdge(fromID, toID)
}

func (g *StreamGraph) ExistsStream(stm Stream) bool {
	node := stm.GetStreamNode()
	if node == nil {
		return false
	}
	if !g.ExistsVertex(node.ID) {
		return false
	}
	return true
}

/*
DAG API
*/

func (g *StreamGraph) ExistsVertex(id int) (ok bool) {
	_, ok = g.Vertices[id]
	return
}

func (g *StreamGraph) GetVertex(id int) *streamNode {
	if v, ok := g.Vertices[id]; ok {
		return v
	}
	return nil
}

func (g *StreamGraph) AddVertex(id int, node *streamNode) error {
	if g.ExistsVertex(id) {
		return errors.Errorf("graph already contains a node with ID %d", id)
	}
	g.Vertices[id] = node
	return nil
}

func (g *StreamGraph) AddEdge(fromID, ToID int) error {
	var from, to *streamNode
	var ok bool

	if from, ok = g.Vertices[fromID]; !ok {
		return errors.Errorf("vertex with the id %v not found, so it can't be added", fromID)
	}

	if to, ok = g.Vertices[ToID]; !ok {
		return errors.Errorf("vertex with the id %v not found, so it can't be added", ToID)
	}

	for _, childVertex := range from.Outgoings {
		if childVertex == to {
			return errors.Errorf("edge (%v,%v) already exists, so it can't be added", fromID, ToID)
		}
	}

	from.Outgoings[to.ID] = to
	to.Incomings[from.ID] = from
	return nil
}
