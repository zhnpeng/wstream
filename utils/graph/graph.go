package graph

type Iterator interface {
	Length() int
	Visit(v int, do func(w int) (skip bool)) (aborted bool)
}
