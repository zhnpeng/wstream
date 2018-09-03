package graph

import (
	"github.com/pkg/errors"
)

const initialMapSize = 4

// Mutable represents a directed graph with a fixed number
// of vertices and weighted outEdges that can be added or removed.
type Mutable struct {
	// The map outEdges[v] contains the mapping {w:c} if there is an edge
	// from v to w, and c is the cost assigned to this edge.
	outEdges []map[int]int64
	inEdges  []map[int]int64 //in-degree
}

// New constructs a new graph with n vertices, numbered from 0 to n-1, and no outEdges.
func New(n int) *Mutable {
	return &Mutable{
		outEdges: make([]map[int]int64, n),
		inEdges:  make([]map[int]int64, n),
	}
}

// String returns a string representation of the graph.
func (g *Mutable) String() string {
	return String(g)
}

// Order returns the number of vertices in the graph.
func (g *Mutable) Order() int {
	return len(g.outEdges)
}

// Visit calls the do function for each neighbor w of v,
// with c equal to the cost of the edge from v to w.
// If do returns true, Visit returns immediately,
// skipping any remaining neighbors, and returns true.
//
// The iteration order is not specified and is not guaranteed
// to be the same every time.
// It is safe to delete, but not to add, outEdges adjacent to v
// during a call to this method.
func (g *Mutable) Visit(v int, do func(w int, c int64) bool) bool {
	for w, c := range g.outEdges[v] {
		if do(w, c) {
			return true
		}
	}
	return false
}

// VisitBoth travel both in-degrees and out-degrees
func (g *Mutable) VisitBoth(v int, doIn func(w int, c int64) bool, doOut func(w int, c int64) bool) bool {
	// visit in-degree
	for w, c := range g.inEdges[v] {
		if doIn(w, c) {
			return true
		}
	}
	// visit out-degree
	for w, c := range g.outEdges[v] {
		if doOut(w, c) {
			return true
		}
	}
	return false
}

// OutDegree returns the number of outward directed edges from v.
func (g *Mutable) OutDegree(v int) int {
	return len(g.outEdges[v])
}

// InDegree returns the number of inward directed edges to v
func (g *Mutable) InDegree(v int) int {
	return len(g.inEdges[v])
}

// Edge tells if there is an edge from v to w.
func (g *Mutable) Edge(v, w int) bool {
	if v < 0 || v >= g.Order() {
		return false
	}
	_, ok := g.outEdges[v][w]
	return ok
}

// Cost returns the cost of an edge from v to w, or 0 if no such edge exists.
func (g *Mutable) Cost(v, w int) int64 {
	if v < 0 || v >= g.Order() {
		return 0
	}
	return g.outEdges[v][w]
}

// AddVertex add a new vertex to the end and return its id
func (g *Mutable) AddVertex() (id int) {
	id = len(g.outEdges)
	g.outEdges = append(g.outEdges, make(map[int]int64, initialMapSize))
	g.inEdges = append(g.inEdges, make(map[int]int64, initialMapSize))
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
	if w < 0 || w >= len(g.outEdges) {
		return errors.Errorf("vertex out of range: %d", w)
	}
	if g.outEdges[v] == nil {
		g.outEdges[v] = make(map[int]int64, initialMapSize)
	}
	if g.inEdges[w] == nil {
		g.inEdges[w] = make(map[int]int64, initialMapSize)
	}
	g.outEdges[v][w] = c
	g.inEdges[w][v] = c
	return nil
}

// AddEdgeBoth inserts outEdges with zero cost between v and w.
// It removes the previous costs if these outEdges already exist.
func (g *Mutable) AddEdgeBoth(v, w int) {
	g.AddEdgeCost(v, w, 0)
	if v != w {
		g.AddEdgeCost(w, v, 0)
	}
}

// AddEdgeBothCost inserts outEdges with cost c between v and w.
// It overwrites the previous costs if these outEdges already exist.
func (g *Mutable) AddEdgeBothCost(v, w int, c int64) {
	g.AddEdgeCost(v, w, c)
	if v != w {
		g.AddEdgeCost(w, v, c)
	}
}

// DeleteEdge removes an edge from v to w.
func (g *Mutable) DeleteEdge(v, w int) {
	delete(g.outEdges[v], w)
}

// DeleteEdgeBoth removes all outEdges between v and w.
func (g *Mutable) DeleteEdgeBoth(v, w int) {
	g.DeleteEdge(v, w)
	if v != w {
		g.DeleteEdge(w, v)
	}
}
