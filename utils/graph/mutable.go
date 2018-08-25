package graph

import (
	"github.com/pkg/errors"
)

type Mutable struct {
	edges [][]int
}

func New() *Mutable {
	return &Mutable{edges: make([][]int, 0)}
}

func (g *Mutable) Length() int {
	return len(g.edges)
}

func (g *Mutable) Visit(v int, do func(w int) bool) bool {
	for _, w := range g.edges[v] {
		if do(w) {
			return true
		}
	}
	return false
}

// Degree return number of outward directed edges from v
func (g *Mutable) Degree(v int) int {
	return len(g.edges[v])
}

func (g *Mutable) AddVertex() (id int) {
	id = len(g.edges)
	g.edges = append(g.edges, make([]int, 0))
	return
}

// AddVertexes add n additional vertexes to graph
// return all index of newly added vertex
func (g *Mutable) AddVertexes(n int) (ids []int) {
	start := len(g.edges)
	for i := 0; i < n; i++ {
		g.edges = append(g.edges, make([]int, 0))
		ids = append(ids, start+i)
	}
	return
}

// AddEdge add edge from v to w
func (g *Mutable) AddEdge(v, w int) error {
	if w < 0 || w >= len(g.edges) {
		return errors.Errorf("vertex out of range: %d", w)
	}
	g.edges[v] = append(g.edges[v], w)
	return nil
}

// AddEdgeBoth add edge for both side (from v to w and w to v)
func (g *Mutable) AddEdgeBoth(v, w int) (err error) {
	err = g.AddEdge(v, w)
	if v != w {
		err = g.AddEdge(w, v)
	}
	return
}

func (g *Mutable) DeleteEdge(v, w int) {
	g.edges[v] = append(g.edges[v][:w], g.edges[v][w+1:]...)
}

func (g *Mutable) DeleteEdgeBoth(v, w int) {
	g.DeleteEdge(v, w)
	if v != w {
		g.DeleteEdge(w, v)
	}
}
