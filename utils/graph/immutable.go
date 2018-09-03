package graph

import (
	"sort"
	"strconv"
)

// Immutable is a compact representation of an immutable graph.
// The implementation uses lists to associate each vertex in the graph
// with its adjacent vertices. This makes for fast and predictable
// iteration: the Visit method produces its elements by reading
// from a fixed sorted precomputed list. This type supports multigraphs.
type Immutable struct {
	// outEdges[v] is a sorted list of v's neighbors.
	outEdges [][]neighbor
	inEdges  [][]neighbor
	stats    Stats
}

type neighbor struct {
	vertex int
	cost   int64
}

// Sort returns an immutable copy of g with a Visit method
// that returns its neighbors in increasing numerical order.
func Sort(g Iterator) *Immutable {
	if g, ok := g.(*Immutable); ok {
		return g
	}
	return build(g, false)
}

// Transpose returns the transpose graph of g.
// The transpose graph has the same set of vertices as g,
// but all of the outEdges are reversed compared to the orientation
// of the corresponding outEdges in g.
func Transpose(g Iterator) *Immutable {
	return build(g, true)
}

func build(g Iterator, transpose bool) *Immutable {
	n := g.Order()
	h := &Immutable{
		outEdges: make([][]neighbor, n),
		inEdges:  make([][]neighbor, n),
	}
	for v := range h.outEdges {
		g.Visit(v, func(w int, c int64) (skip bool) {
			if w < 0 || w >= n {
				panic("vertex out of range: " + strconv.Itoa(w))
			}
			if transpose {
				h.outEdges[w] = append(h.outEdges[w], neighbor{v, c})
				h.inEdges[v] = append(h.inEdges[v], neighbor{w, c})
			} else {
				h.outEdges[v] = append(h.outEdges[v], neighbor{w, c})
				h.inEdges[w] = append(h.inEdges[w], neighbor{v, c})
			}
			return
		})
		sort.Slice(h.outEdges[v], func(i, j int) bool {
			if e := h.outEdges[v]; e[i].vertex == e[j].vertex {
				return e[i].cost < e[j].cost
			} else {
				return e[i].vertex < e[j].vertex
			}
		})
		sort.Slice(h.inEdges[v], func(i, j int) bool {
			if e := h.inEdges[v]; e[i].vertex == e[j].vertex {
				return e[i].cost < e[j].cost
			} else {
				return e[i].vertex < e[j].vertex
			}
		})
	}
	// stats outEdges only cause inEdges are redundance
	for v, neighbors := range h.outEdges {
		if len(neighbors) == 0 {
			h.stats.Isolated++
		}
		prev := -1
		for _, e := range neighbors {
			w, c := e.vertex, e.cost
			if v == w {
				h.stats.Loops++
			}
			if c != 0 {
				h.stats.Weighted++
			}
			if w == prev {
				h.stats.Multi++
			} else {
				h.stats.Size++
				prev = w
			}
		}
	}
	return h
}

// Visit calls the do function for each neighbor w of v,
// with c equal to the cost of the edge from v to w.
// The neighbors are visited in increasing numerical order.
// If do returns true, Visit returns immediately,
// skipping any remaining neighbors, and returns true.
func (g *Immutable) Visit(v int, do func(w int, c int64) bool) bool {
	for _, e := range g.outEdges[v] {
		if do(e.vertex, e.cost) {
			return true
		}
	}
	return false
}

// VisitBoth travel both in-degrees and out-degrees
func (g *Immutable) VisitBoth(v int, doIn func(w int, c int64) bool, doOut func(w int, c int64) bool) bool {
	for _, e := range g.inEdges[v] {
		if doIn(e.vertex, e.cost) {
			return true
		}
	}
	for _, e := range g.outEdges[v] {
		if doOut(e.vertex, e.cost) {
			return true
		}
	}
	return false
}

// VisitFrom calls the do function starting from the first neighbor w
// for which w ≥ a, with c equal to the cost of the edge from v to w.
// The neighbors are then visited in increasing numerical order.
// If do returns true, VisitFrom returns immediately,
// skipping any remaining neighbors, and returns true.
func (g *Immutable) VisitFrom(v int, a int, do func(w int, c int64) bool) bool {
	neighbors := g.outEdges[v]
	n := len(neighbors)
	i := sort.Search(n, func(i int) bool { return a <= neighbors[i].vertex })
	for ; i < n; i++ {
		e := neighbors[i]
		if do(e.vertex, e.cost) {
			return true
		}
	}
	return false
}

// String returns a string representation of the graph.
func (g *Immutable) String() string {
	return String(g)
}

// Order returns the number of vertices in the graph.
func (g *Immutable) Order() int {
	return len(g.outEdges)
}

// Edge tells if there is an edge from v to w.
func (g *Immutable) Edge(v, w int) bool {
	if v < 0 || v >= len(g.outEdges) {
		return false
	}
	outEdges := g.outEdges[v]
	n := len(outEdges)
	i := sort.Search(n, func(i int) bool { return w <= outEdges[i].vertex })
	return i < n && w == outEdges[i].vertex
}

// Degree returns the number of outward directed outEdges from v.
func (g *Immutable) Degree(v int) int {
	return len(g.outEdges[v])
}
