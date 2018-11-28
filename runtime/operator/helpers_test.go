package operator

import (
	"testing"
)

func Test_encodeFunction(t *testing.T) {
	type tfunc struct {
		Name string
	}
	encodeFunction(&tfunc{})
}
