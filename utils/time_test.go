package utils

import (
	"testing"
)

func TestParseTime(t *testing.T) {
	ParseTime("2018-11-25 07:30:00")
}

func TestParseTimeMilli(t *testing.T) {
	ParseTimeMilli("2018-11-25 07:30:00.100")
}
