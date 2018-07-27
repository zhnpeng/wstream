package functions

type MapFunc interface {
	Map(i interface{}) interface{}
}

type MapFunction struct {
	parameters map[interface{}]interface{}
}

func (m *MapFunction) Map(i interface{}) interface{} {
	return i
}
