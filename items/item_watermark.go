package items

//go:generate msgp

import (
	"time"
)

type Watermark struct {
	T time.Time
}
