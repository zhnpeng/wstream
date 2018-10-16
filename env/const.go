package env

type TypeTimeCharacteristic int

const (
	IsEventTime TypeTimeCharacteristic = iota
	IsProcessingTime
)
