package execution

type Node interface {
	AddInEdge(InEdge)
	AddOutEdge(OutEdge)
	Run()
}
