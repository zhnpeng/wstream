package utils

import (
	"fmt"

	"github.com/OneOfOne/xxhash"
)

func Hash(bytes []byte) uint32 {
	h := xxhash.New32()
	h.Write(bytes)
	return h.Sum32()
}

type KeyID string

func HashSlice(slice []interface{}) KeyID {
	return KeyID(fmt.Sprintf("%v", slice))
}
