package global

// Environment is for global varialbe
type Environment struct {
	TimeCharacteristic TypeTimeCharacteristic
}

func NewEnvironment() *Environment {
	return &Environment{}
}

// ENV is global environment
var ENV Environment
