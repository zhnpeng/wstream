package graph

import (
	"math/rand"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func SetUpImm() (g0, g1, g1c, g5, g5c *Immutable) {
	g0 = Sort(New(0))

	g := New(1)
	g.AddEdge(0, 0)
	g1 = Sort(g)

	g.AddEdgeCost(0, 0, 1)
	g1c = Sort(g)

	g = New(5)
	g.AddEdge(0, 1)
	g.AddEdge(2, 3)
	g5 = Sort(g)

	g.AddEdgeCost(2, 3, 1)
	g5c = Sort(g)
	return
}

func TestSort(t *testing.T) {
	g0, g1, g1c, g5, g5c := SetUpImm()

	res := g0
	exp := "0 []"
	if diff := cmp.Diff(res.String(), exp); diff != "" {
		t.Errorf("Sort: %s", diff)
	}

	res = g1
	exp = "1 [(0 0)]"
	if diff := cmp.Diff(res.String(), exp); diff != "" {
		t.Errorf("Sort: %s", diff)
	}

	res = g1c
	exp = "1 [(0 0):1]"
	if diff := cmp.Diff(res.String(), exp); diff != "" {
		t.Errorf("Sort: %s", diff)
	}

	res = g5
	exp = "5 [(0 1) (2 3)]"
	if diff := cmp.Diff(res.String(), exp); diff != "" {
		t.Errorf("Sort: %s", diff)
	}

	res = g5c
	exp = "5 [(0 1) (2 3):1]"
	if diff := cmp.Diff(res.String(), exp); diff != "" {
		t.Errorf("Sort: %s", diff)
	}

	res = Sort(g5c)
	exp = "5 [(0 1) (2 3):1]"
	if diff := cmp.Diff(res.String(), exp); diff != "" {
		t.Errorf("Sort: %s", diff)
	}

	n := 10
	g := New(n)
	for i := 0; i < 2*n; i++ {
		g.AddEdgeBothCost(rand.Intn(n), rand.Intn(n), rand.Int63())
	}
}

func TestTranspose(t *testing.T) {
	g0, g1, g1c, g5, g5c := SetUpImm()

	res := Transpose(g0)
	exp := "0 []"
	if diff := cmp.Diff(res.String(), exp); diff != "" {
		t.Errorf("Transpose: %s", diff)
	}

	res = Transpose(g1)
	exp = "1 [(0 0)]"
	if diff := cmp.Diff(res.String(), exp); diff != "" {
		t.Errorf("Transpose g1: %s", diff)
	}

	res = Transpose(g1c)
	exp = "1 [(0 0):1]"
	if diff := cmp.Diff(res.String(), exp); diff != "" {
		t.Errorf("Transpose g1c: %s", diff)
	}

	res = Transpose(g5)
	exp = "5 [(1 0) (3 2)]"
	if diff := cmp.Diff(res.String(), exp); diff != "" {
		t.Errorf("Transpose: %s", diff)
	}

	res = Transpose(g5c)
	exp = "5 [(1 0) (3 2):1]"
	if diff := cmp.Diff(res.String(), exp); diff != "" {
		t.Errorf("Transpose: %s", diff)
	}

	n := 10
	g := New(n)
	for i := 0; i < 2*n; i++ {
		g.AddEdgeBothCost(rand.Intn(n), rand.Intn(n), rand.Int63())
	}
}

func TestOrderImm(t *testing.T) {
	g0, g1, g1c, g5, g5c := SetUpImm()
	s := "Order()"

	if diff := cmp.Diff(g0.Order(), 0); diff != "" {
		t.Errorf("g0.%s %s", s, diff)
	}
	if diff := cmp.Diff(g1.Order(), 1); diff != "" {
		t.Errorf("g1.%s %s", s, diff)
	}
	if diff := cmp.Diff(g5.Order(), 5); diff != "" {
		t.Errorf("g5.%s %s", s, diff)
	}
	if diff := cmp.Diff(g1c.Order(), 1); diff != "" {
		t.Errorf("g1.%s %s", s, diff)
	}
	if diff := cmp.Diff(g5c.Order(), 5); diff != "" {
		t.Errorf("g5.%s %s", s, diff)
	}
}

func TestEdgeImm(t *testing.T) {
	_, g1, g1c, g5, _ := SetUpImm()

	if diff := cmp.Diff(g1.Edge(-1, 0), false); diff != "" {
		t.Errorf("g1.Edge(-1, 0) %s", diff)
	}
	if diff := cmp.Diff(g1.Edge(1, 0), false); diff != "" {
		t.Errorf("g1.Edge(1, 0) %s", diff)
	}
	if diff := cmp.Diff(g1.Edge(0, 0), true); diff != "" {
		t.Errorf("g1.Edge(0, 0) %s", diff)
	}
	if diff := cmp.Diff(g1c.Edge(0, 0), true); diff != "" {
		t.Errorf("g1c.Edge(0, 0) %s", diff)
	}
	if diff := cmp.Diff(g5.Edge(0, 1), true); diff != "" {
		t.Errorf("g5.Edge(0, 1) %s", diff)
	}
	if diff := cmp.Diff(g5.Edge(1, 0), false); diff != "" {
		t.Errorf("g5.Edge(1, 0) %s", diff)
	}
	if diff := cmp.Diff(g5.Edge(2, 3), true); diff != "" {
		t.Errorf("g5.Edge(2, 3) %s", diff)
	}
	if diff := cmp.Diff(g5.Edge(3, 2), false); diff != "" {
		t.Errorf("g5.Edge(3, 2) %s", diff)
	}
}

func TestDegreeImm(t *testing.T) {
	_, g1, g1c, g5, g5c := SetUpImm()

	if diff := cmp.Diff(g1.Degree(0), 1); diff != "" {
		t.Errorf("g1.Degree(0) %s", diff)
	}
	if diff := cmp.Diff(g1c.Degree(0), 1); diff != "" {
		t.Errorf("g1c.Degree(0) %s", diff)
	}
	if diff := cmp.Diff(g5.Degree(0), 1); diff != "" {
		t.Errorf("g5.Degree(0) %s", diff)
	}
	if diff := cmp.Diff(g5c.Degree(0), 1); diff != "" {
		t.Errorf("g5c.Degree(0) %s", diff)
	}
	if diff := cmp.Diff(g5.Degree(1), 0); diff != "" {
		t.Errorf("g5.Degree(1) %s", diff)
	}
	if diff := cmp.Diff(g5c.Degree(1), 0); diff != "" {
		t.Errorf("g5c.Degree(1) %s", diff)
	}
}
