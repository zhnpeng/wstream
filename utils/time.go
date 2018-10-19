package utils

import "time"

// ParseTime parse time in string format "2006-01-02 15:04:05"
func ParseTime(value string) time.Time {
	tsLayout := "2006-01-02 15:04:05"
	ts, err := time.ParseInLocation(tsLayout, value, time.Local)
	if err != nil {
		panic(err)
	}
	return ts
}

// ParseTimeMilli parse time in string format "2006-01-02 15:04:05.000"
func ParseTimeMilli(value string) time.Time {
	tsLayout := "2006-01-02 15:04:05.000"
	ts, err := time.ParseInLocation(tsLayout, value, time.Local)
	if err != nil {
		panic(err)
	}
	return ts
}
