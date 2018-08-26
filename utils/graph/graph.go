package graph

import (
	"sort"
	"strconv"
)

// Iterator is a non-weighted graph; an Iterator can be used
// to describe both ordinary graphs and multigraphs
type Iterator interface {
	Order() int
	Visit(v int, do func(w int, c int64) (skip bool)) (aborted bool)
}

// Stats holds basic data about an Iterator.
type Stats struct {
	Size     int // Number of unique edges.
	Multi    int // Number of duplicate edges.
	Weighted int // Number of edges with non-zero cost.
	Loops    int // Number of self-loops.
	Isolated int // Number of vertices with outdegree zero.
}

// Check collects data about an Iterator.
func Check(g Iterator) Stats {
	_, mutable := g.(*Mutable)

	n := g.Order()
	degree := make([]int, n)
	type edge struct{ v, w int }
	edges := make(map[edge]bool)
	var stats Stats
	for v := 0; v < n; v++ {
		g.Visit(v, func(w int, c int64) (skip bool) {
			if w < 0 || w >= n {
				panic("vertex out of range: " + strconv.Itoa(w))
			}
			if v == w {
				stats.Loops++
			}
			if c != 0 {
				stats.Weighted++
			}
			degree[v]++
			if mutable { // A Mutable is never a multigraph.
				stats.Size++
				return
			}
			if edges[edge{v, w}] {
				stats.Multi++
			} else {
				stats.Size++
			}
			edges[edge{v, w}] = true
			return
		})
	}
	for _, deg := range degree {
		if deg == 0 {
			stats.Isolated++
		}
	}
	return stats
}

// The maximum and minum value of an edge cost.
const (
	Max int64 = 1<<63 - 1
	Min int64 = -1 << 63
)

type edge struct {
	v, w int
	c    int64
}

// String returns a description of g with two elements:
// the number of vertices, followed by a sorted list of all edges.
func String(g Iterator) string {
	n := g.Order()
	// This may be a multigraph, so we look for duplicates by counting.
	count := make(map[edge]int)
	for v := 0; v < n; v++ {
		g.Visit(v, func(w int, c int64) (skip bool) {
			count[edge{v, w, c}]++
			return
		})
	}
	edges := make([]edge, 0, len(count))
	for e := range count {
		edges = append(edges, e)
	}
	// Sort lexicographically on (v, w, c).
	sort.Slice(edges, func(i, j int) bool {
		v := edges[i].v == edges[j].v
		w := edges[i].w == edges[j].w
		switch {
		case v && w:
			return edges[i].c < edges[j].c
		case v:
			return edges[i].w < edges[j].w
		default:
			return edges[i].v < edges[j].v
		}
	})
	// Build the string.
	var buf []byte
	buf = strconv.AppendInt(buf, int64(n), 10)
	buf = append(buf, " ["...)
	for _, e := range edges {
		c := count[e]
		if e.v < e.w {
			// Collect edges in opposite directions into an undirected edge.
			back := edge{e.w, e.v, e.c}
			m := min(c, count[back])
			count[back] -= m
			buf = appendEdge(buf, e, m, true)
			buf = appendEdge(buf, e, c-m, false)
		} else {
			buf = appendEdge(buf, e, c, false)
		}
	}
	if len(edges) > 0 {
		buf = buf[:len(buf)-1] // Remove trailing ' '.
	}
	buf = append(buf, ']')
	return string(buf)
}

func appendEdge(buf []byte, e edge, count int, bi bool) []byte {
	if count <= 0 {
		return buf
	}
	if count > 1 {
		buf = strconv.AppendInt(buf, int64(count), 10)
		buf = append(buf, "×"...)
	}
	if bi {
		buf = append(buf, '{')
	} else {
		buf = append(buf, '(')
	}
	buf = strconv.AppendInt(buf, int64(e.v), 10)
	buf = append(buf, ' ')
	buf = strconv.AppendInt(buf, int64(e.w), 10)
	if bi {
		buf = append(buf, '}')
	} else {
		buf = append(buf, ')')
	}
	if e.c != 0 {
		buf = append(buf, ':')
		switch e.c {
		case Max:
			buf = append(buf, "max"...)
		case Min:
			buf = append(buf, "min"...)
		default:
			buf = strconv.AppendInt(buf, e.c, 10)
		}
	}
	buf = append(buf, ' ')
	return buf
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
