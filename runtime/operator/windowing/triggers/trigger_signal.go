package triggers

// TriggerSignal is signal of trigger
type TriggerSignal int

const (
	FIRE TriggerSignal = iota
	PURGE
	FIREANDPURGE
	CONTINUE
)

func (s TriggerSignal) IsFire() bool {
	if s == FIRE || s == FIREANDPURGE {
		return true
	}
	return false
}

func (s TriggerSignal) IsPurge() bool {
	if s == PURGE || s == FIREANDPURGE {
		return true
	}
	return false
}
