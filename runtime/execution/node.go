package execution

type Node interface {
	AddInEdge(inEdge InEdge)
	AddOutEdge(outEdge OutEdge)
	Run()
}
