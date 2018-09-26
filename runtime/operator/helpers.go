package operator

import (
	"bytes"
	"encoding/gob"
)

func encodeFunction(function interface{}) []byte {
	var buf bytes.Buffer
	gob.Register(function)
	gob.NewEncoder(&buf).Encode(&function)
	return buf.Bytes()
}
