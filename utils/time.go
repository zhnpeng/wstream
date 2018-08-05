package utils

import "time"

func TimeParse(value string) time.Time {
	tsLayout := "2006-01-02 15:04:05"
	ts, err := time.ParseInLocation(tsLayout, value, time.Local)
	if err != nil {
		panic(err)
	}
	return ts
}

func MilliTimeParse(value string) time.Time {
	tsLayout := "2006-01-02 15:04:05.000"
	ts, err := time.ParseInLocation(tsLayout, value, time.Local)
	if err != nil {
		panic(err)
	}
	return ts
}
