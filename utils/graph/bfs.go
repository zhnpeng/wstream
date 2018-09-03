package graph

// BFS traverses g in breadth-first order starting at v.
// When the algorithm follows an edge (v, w) and finds a previously
// unvisited vertex w, it calls do(v, w, c) with c equal to
// the cost of the edge (v, w).
func BFS(g Iterator, v int, do func(v, w int, c int64)) {
	visited := make([]bool, g.Order())
	visited[v] = true
	for queue := []int{v}; len(queue) > 0; {
		v := queue[0]
		queue = queue[1:]
		g.Visit(v, func(w int, c int64) (skip bool) {
			if visited[w] {
				return
			}
			do(v, w, c)
			visited[w] = true
			queue = append(queue, w)
			return
		})
	}
}

// BFSBoth travel both in-degree and out-degree
// TODO: not elegant enought, refine me
func BFSBoth(g Iterator, v int, do func(v, w int, c int64)) {
	visited := make([]bool, g.Order())
	visited[v] = true
	for queue := []int{v}; len(queue) > 0; {
		v := queue[0]
		queue = queue[1:]
		g.VisitBoth(
			v,
			func(w int, c int64) (skip bool) {
				if visited[w] {
					return
				}
				do(v, w, c)
				visited[w] = true
				queue = append(queue, w)
				return
			},
			func(w int, c int64) (skip bool) {
				if visited[w] {
					return
				}
				do(v, w, c)
				visited[w] = true
				queue = append(queue, w)
				return
			},
		)
	}
}
