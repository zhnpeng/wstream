package triggers

// TriggerSignal is signal of trigger
type TriggerSignal int

const (
	FIRE TriggerSignal = iota
	CONTINUE
)

func (s TriggerSignal) IsFire() bool {
	return s == FIRE
}

func (s TriggerSignal) String() string {
	switch s {
	case FIRE:
		return "Fire"
	case CONTINUE:
		return "Continue"
	default:
		return "Invalid Signal"
	}
}
