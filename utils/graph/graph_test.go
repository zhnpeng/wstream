package graph

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestCheck(t *testing.T) {
	g := New(0)
	res := Check(g)
	exp := Stats{0, 0, 0, 0, 0}
	if diff := cmp.Diff(res, exp); diff != "" {
		t.Errorf("Check: %s", diff)
	}

	g = New(1)
	res = Check(g)
	exp = Stats{0, 0, 0, 0, 1}
	if diff := cmp.Diff(res, exp); diff != "" {
		t.Errorf("Check: %s", diff)
	}

	g.AddVertex()
	g.AddEdgeCost(0, 1, 1)
	res = Check(g)
	exp = Stats{1, 0, 1, 0, 1}
	if diff := cmp.Diff(res, exp); diff != "" {
		t.Errorf("Check: %s", diff)
	}

	g = New(1)
	g.AddEdgeCost(0, 0, 1)
	res = Check(g)
	exp = Stats{1, 0, 1, 1, 0}
	if diff := cmp.Diff(res, exp); diff != "" {
		t.Errorf("Check %s", diff)
	}

	g = New(3)
	g.AddEdgeCost(0, 1, 1)
	res = Check(g)
	exp = Stats{1, 0, 1, 0, 2}
	if diff := cmp.Diff(res, exp); diff != "" {
		t.Errorf("Check: %s", diff)
	}

	g.AddEdgeCost(1, 0, 2)
	res = Check(g)
	exp = Stats{2, 0, 2, 0, 1}
	if diff := cmp.Diff(res, exp); diff != "" {
		t.Errorf("Check: %s", diff)
	}

	g.AddEdgeCost(1, 0, 1)
	res = Check(g)
	exp = Stats{2, 0, 2, 0, 1}
	if diff := cmp.Diff(res, exp); diff != "" {
		t.Errorf("Check: %s", diff)
	}

	g.AddEdge(2, 2)
	res = Check(g)
	exp = Stats{3, 0, 2, 1, 0}
	if diff := cmp.Diff(res, exp); diff != "" {
		t.Errorf("Check: %s", diff)
	}

	g.AddEdge(1, 2)
	res = Check(g)
	exp = Stats{4, 0, 2, 1, 0}
	if diff := cmp.Diff(res, exp); diff != "" {
		t.Errorf("Check: %s", diff)
	}

	g.AddEdgeBoth(0, 1)
	res = Check(g)
	exp = Stats{4, 0, 0, 1, 0}
	if diff := cmp.Diff(res, exp); diff != "" {
		t.Errorf("Check: %s", diff)
	}

	g.AddVertexes(2)
	res = Check(g)
	exp = Stats{4, 0, 0, 1, 2}
	if diff := cmp.Diff(res, exp); diff != "" {
		t.Errorf("Check: %s", diff)
	}
}
