package types

// Record is kind of item
type Record interface {
	Item
	Copy() Record
	Get(index interface{}) interface{}
	GetMany(indexes ...interface{}) []interface{}
	Set(index, value interface{}) error
	UseKeys(indexes ...interface{}) []interface{}
	Key() []interface{}
	Inherit(Record) Record
}
