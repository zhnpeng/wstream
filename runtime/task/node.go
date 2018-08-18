package task

type Node interface {
	Type() NodeType
	AddInEdge(inEdge InEdge)
	AddOutEdge(outEdge OutEdge)
	Run()
}
