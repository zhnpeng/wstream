package intfs

type Iterator interface {
	Next() Iterator
	Prev() Iterator
	Value() interface{}
}
