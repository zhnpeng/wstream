package com

import (
	"github.com/sirupsen/logrus"
)

func CatchPanic() {
	if r := recover(); r != nil {
		logrus.Errorf("panic cause of: %v\n", r)
	}
}
