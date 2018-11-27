package types

// Record is kind of item
type Record interface {
	Item
	Get(index interface{}) interface{}
	GetMany(indexes ...interface{}) []interface{}
	Set(index, value interface{}) error
	UseKeys(indexes ...interface{}) []interface{}
	Key() []interface{}

	SetKey([]interface{})
	// Inherit inherit T and K from another record
	Inherit(Record) Record
}
