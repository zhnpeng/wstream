package global

type TypeTimeCharacteristic int

const (
	IsEventTime TypeTimeCharacteristic = iota
	IsProcessingTime
)
