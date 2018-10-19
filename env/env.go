package env

import "sync"

// Environment is for global varialbe
type Environment struct {
	TimeCharacteristic TypeTimeCharacteristic
}

// ENV is global environment
var e *Environment
var once sync.Once

// Env return global environment
func Env() *Environment {
	once.Do(func() {
		e = &Environment{}
	})
	return e
}
