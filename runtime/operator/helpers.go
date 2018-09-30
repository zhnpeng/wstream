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

func hashSlice(slice []interface{}) string {
	return fmt.Sprintf("%v", slice)
}
