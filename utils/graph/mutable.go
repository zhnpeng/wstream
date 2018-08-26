package graph

import (
	"github.com/pkg/errors"
)

const initialMapSize = 4

// Mutable represents a directed graph with a fixed number
// of vertices and weighted edges that can be added or removed.
// The implementation uses hash maps to associate each vertex in the graph with
// its adjacent vertices. This gives constant time performance for
// all basic operations.
//
type Mutable struct {
	// The map edges[v] contains the mapping {w:c} if there is an edge
	// from v to w, and c is the cost assigned to this edge.
	// The maps may be nil and are allocated as needed.
	edges []map[int]int64
}

// New constructs a new graph with n vertices, numbered from 0 to n-1, and no edges.
func New(n int) *Mutable {
	return &Mutable{edges: make([]map[int]int64, n)}
}

// String returns a string representation of the graph.
func (g *Mutable) String() string {
	return String(g)
}

// Order returns the number of vertices in the graph.
func (g *Mutable) Order() int {
	return len(g.edges)
}

// Visit calls the do function for each neighbor w of v,
// with c equal to the cost of the edge from v to w.
// If do returns true, Visit returns immediately,
// skipping any remaining neighbors, and returns true.
//
// The iteration order is not specified and is not guaranteed
// to be the same every time.
// It is safe to delete, but not to add, edges adjacent to v
// during a call to this method.
func (g *Mutable) Visit(v int, do func(w int, c int64) bool) bool {
	for w, c := range g.edges[v] {
		if do(w, c) {
			return true
		}
	}
	return false
}

// Degree returns the number of outward directed edges from v.
func (g *Mutable) Degree(v int) int {
	return len(g.edges[v])
}

// Edge tells if there is an edge from v to w.
func (g *Mutable) Edge(v, w int) bool {
	if v < 0 || v >= g.Order() {
		return false
	}
	_, ok := g.edges[v][w]
	return ok
}

// Cost returns the cost of an edge from v to w, or 0 if no such edge exists.
func (g *Mutable) Cost(v, w int) int64 {
	if v < 0 || v >= g.Order() {
		return 0
	}
	return g.edges[v][w]
}

// AddVertex add a new vertex to the end and return its id
func (g *Mutable) AddVertex() (id int) {
	id = len(g.edges)
	g.edges = append(g.edges, make(map[int]int64, initialMapSize))
	return
}

// AddVertexes add n vertexex to graph and return all newly added ids
func (g *Mutable) AddVertexes(n int) (ids []int) {
	for i := 0; i < n; i++ {
		id := g.AddVertex()
		ids = append(ids, id)
	}
	return
}

// AddEdge inserts a directed edge from v to w with zero cost.
// It removes the previous cost if this edge already exists.
func (g *Mutable) AddEdge(v, w int) error {
	return g.AddEdgeCost(v, w, 0)
}

// AddEdgeCost inserts a directed edge from v to w with cost c.
// It overwrites the previous cost if this edge already exists.
func (g *Mutable) AddEdgeCost(v, w int, c int64) error {
	// Make sure not to break internal state.
	if w < 0 || w >= len(g.edges) {
		return errors.Errorf("vertex out of range: %d", w)
	}
	if g.edges[v] == nil {
		g.edges[v] = make(map[int]int64, initialMapSize)
	}
	g.edges[v][w] = c
	return nil
}

// AddEdgeBoth inserts edges with zero cost between v and w.
// It removes the previous costs if these edges already exist.
func (g *Mutable) AddEdgeBoth(v, w int) {
	g.AddEdgeCost(v, w, 0)
	if v != w {
		g.AddEdgeCost(w, v, 0)
	}
}

// AddEdgeBothCost inserts edges with cost c between v and w.
// It overwrites the previous costs if these edges already exist.
func (g *Mutable) AddEdgeBothCost(v, w int, c int64) {
	g.AddEdgeCost(v, w, c)
	if v != w {
		g.AddEdgeCost(w, v, c)
	}
}

// DeleteEdge removes an edge from v to w.
func (g *Mutable) DeleteEdge(v, w int) {
	delete(g.edges[v], w)
}

// DeleteEdgeBoth removes all edges between v and w.
func (g *Mutable) DeleteEdgeBoth(v, w int) {
	g.DeleteEdge(v, w)
	if v != w {
		g.DeleteEdge(w, v)
	}
}
