package env

// Environment is for global varialbe
type Environment struct {
	TimeCharacteristic TypeTimeCharacteristic
}

// New an Environment
func New() *Environment {
	return &Environment{}
}

// ENV is global environment
var ENV Environment
