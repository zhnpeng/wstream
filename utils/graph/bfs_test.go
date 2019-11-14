package graph

import (
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestBFS(t *testing.T) {
	g := New(10)
	for _, e := range []struct {
		v, w int
	}{
		{0, 1}, {0, 4}, {0, 7}, {0, 9},
		{4, 2}, {7, 5}, {7, 8},
		{2, 3}, {5, 6},
		{3, 6}, {8, 9}, {4, 4},
	} {
		g.AddEdgeBoth(e.v, e.w)
	}
	exp := "0147925836"
	res := "0"
	BFS(Sort(g), 0, func(v, w int, c int64) {
		res += strconv.Itoa(w)
	})
	if diff := cmp.Diff(res, exp); diff != "" {
		t.Errorf("BFS: %s", diff)
	}
}

func TestBFSBoth(t *testing.T) {
	g := New(10)
	for _, e := range []struct {
		v, w int
	}{
		{0, 1}, {0, 4}, {0, 7}, {0, 9},
		{4, 2}, {7, 5}, {7, 8},
		{2, 3}, {5, 6},
		{3, 6}, {8, 9}, {4, 4},
	} {
		g.AddEdge(e.v, e.w)
	}
	exp := "7058149623"
	res := "7"
	BFSBoth(Sort(g), 7, func(v, w int, c int64) {
		res += strconv.Itoa(w)
	})
	if diff := cmp.Diff(res, exp); diff != "" {
		t.Errorf("BFS: %s", diff)
	}
}

func TestBFSAll(t *testing.T) {
	g := New(10)
	for _, e := range []struct {
		v, w int
	}{
		{0, 1}, {0, 4}, {0, 7}, {0, 9},
		{4, 2}, {7, 5}, {7, 8},
		{2, 3}, {5, 6},
		{3, 6}, {8, 9}, {4, 4},
	} {
		g.AddEdge(e.v, e.w)
	}
	got := make([]struct{ v, w int }, 0)
	expected := []struct {
		v, w int
	}{
		{7, 5}, {7, 8},
		{0, 1}, {0, 4}, {0, 7}, {0, 9},
		{5, 6}, {8, 9},
		{4, 2}, {4, 4},
		{2, 3}, {3, 6},
	}
	visited := make([]bool, g.Order())
	exp := "7580149623"
	res := ""
	BFSAll(Sort(g), 7, func(v, w int, c int64) {
		got = append(got, struct{ v, w int }{v, w})
		if !visited[v] {
			visited[v] = true
			res += strconv.Itoa(v)
		}
		if !visited[w] {
			visited[w] = true
			res += strconv.Itoa(w)
		}
	})
	if diff := cmp.Diff(res, exp); diff != "" {
		t.Errorf("BFSAll: %s", diff)
	}
	if len(got) != 12 {
		t.Errorf("BFSAll want: %d, got: %d", 12, len(got))
	}
	for i := 0; i < 12; i++ {
		if got[i].v != expected[i].v || got[i].w != expected[i].w {
			t.Errorf("BFSALL: got: %v, wnat: %v", got, expected)
			break
		}
	}
}
