package execution

type Node interface {
	Type() NodeType
	AddInEdge(inEdge InEdge)
	AddOutEdge(outEdge OutEdge)
	Run()
}
