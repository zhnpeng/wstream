package execution

import (
	"sync"

	"github.com/pkg/errors"
)

// GraphNode is a wrapper for execution node
type GraphNode struct {
	id        int
	value     Node
	outgoings map[int]*GraphNode
	incomings map[int]*GraphNode
}

func newGraphNode(id int, node Node) *GraphNode {
	return &GraphNode{
		id:        id,
		value:     node,
		outgoings: make(map[int]*GraphNode),
		incomings: make(map[int]*GraphNode),
	}
}

type ExecutionGraph struct {
	Vertices map[int]*GraphNode
	length   int
	mu       sync.Mutex
}

func NewExecutionGraph() *ExecutionGraph {
	return &ExecutionGraph{
		Vertices: make(map[int]*GraphNode),
	}
}

func (g *ExecutionGraph) Length() int {
	return g.length
}

func (g *ExecutionGraph) AddNode(node Node) (*GraphNode, error) {
	g.mu.Lock()
	id := g.length
	g.length++
	g.mu.Unlock()
	gNode := newGraphNode(id, node)
	return gNode, g.AddVertex(id, gNode)
}

/*
DAG API
*/

func (g *ExecutionGraph) ExistsVertex(id int) (ok bool) {
	_, ok = g.Vertices[id]
	return
}

func (g *ExecutionGraph) GetVertex(id int) *GraphNode {
	if v, ok := g.Vertices[id]; ok {
		return v
	}
	return nil
}

func (g *ExecutionGraph) AddVertex(id int, node *GraphNode) error {
	if g.ExistsVertex(id) {
		return errors.Errorf("graph already contains a node with ID %d", id)
	}
	g.Vertices[id] = node
	return nil
}

func (g *ExecutionGraph) AddEdge(fromID, toID int) error {
	var from, to *GraphNode
	var ok bool

	if from, ok = g.Vertices[fromID]; !ok {
		return errors.Errorf("vertex with the id %v not found, so it can't be added", fromID)
	}

	if to, ok = g.Vertices[toID]; !ok {
		return errors.Errorf("vertex with the id %v not found, so it can't be added", toID)
	}

	for _, childVertex := range from.outgoings {
		if childVertex == to {
			return errors.Errorf("edge (%v,%v) already exists, so it can't be added", fromID, toID)
		}
	}

	from.outgoings[to.id] = to
	to.incomings[from.id] = from
	return nil
}
