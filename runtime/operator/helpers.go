package operator

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

func encodeFunction(function interface{}) []byte {
	var buf bytes.Buffer
	gob.Register(function)
	gob.NewEncoder(&buf).Encode(&function)
	return buf.Bytes()
}

func hashSlice(slice []interface{}) string {
	return fmt.Sprintf("%v", slice)
}
