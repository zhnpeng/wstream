package com

import (
	"sync"
	"testing"
)

func TestCatchPanic(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer CatchPanic()
		panic("this is a panic!")
	}()
	wg.Wait()
}
