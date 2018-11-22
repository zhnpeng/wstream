package selector

import (
	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/types"
	"github.com/wandouz/wstream/utils"
)

// KeyBy is a Selector rescale partitions according to record's key
type KeyBy struct {
	keys []interface{}
}

func NewKeyBy(keys []interface{}) *KeyBy {
	return &KeyBy{
		keys: keys,
	}
}

func (m *KeyBy) New() intfs.Selector {
	return NewKeyBy(m.keys)
}

func (m *KeyBy) Select(record types.Record, size int) int {
	kvs := record.UseKeys(m.keys...)
	index := utils.PartitionByKeys(size, kvs)
	return index
}
