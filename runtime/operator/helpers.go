package operator

import (
	"bytes"
	"encoding/gob"
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
