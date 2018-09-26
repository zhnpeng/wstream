package operator

import (
	"bytes"
	"encoding/gob"
)

func encodeFunction(function interface{}) *bytes.Reader {
	var buf bytes.Buffer
	gob.Register(function)
	gob.NewEncoder(&buf).Encode(function)
	reader := bytes.NewReader(buf.Bytes())
	return reader
}
