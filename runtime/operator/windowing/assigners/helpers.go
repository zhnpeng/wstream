package assigners

// GetWindowStartWithOffset return last start time contain slide seconds
func GetWindowStartWithOffset(ts, offset, slide int64) int64 {
	return (ts-offset*3600)/slide*slide + offset*3600
}
