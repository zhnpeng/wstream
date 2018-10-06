package operator

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

func encodeFunction(function interface{}) []byte {
	var buf bytes.Buffer
	gob.Register(function)
	err := gob.NewEncoder(&buf).Encode(&function)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

type KeyID string

func hashSlice(slice []interface{}) KeyID {
	return KeyID(fmt.Sprintf("%v", slice))
}
