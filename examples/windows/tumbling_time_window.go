package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/spf13/cast"
	"github.com/wandouz/wstream/stream"
	"github.com/wandouz/wstream/types"
	"github.com/wandouz/wstream/utils"
)

type myMapFunc struct{}

func (tmf *myMapFunc) Map(r types.Record) (o types.Record) {
	x := cast.ToInt(r.Get("X"))
	_ = r.Set("X", x+1)
	return r
}

type myReduceFunc struct{}

func (trf *myReduceFunc) Accmulater(a types.Record) types.Record {
	return types.NewRawMapRecord(
		map[string]interface{}{
			"X": cast.ToInt(a.Get("X")),
		},
	)
}

func (trf *myReduceFunc) Reduce(a, b types.Record) types.Record {
	x := cast.ToInt(a.Get("X")) + cast.ToInt(b.Get("X"))
	return types.NewRawMapRecord(
		map[string]interface{}{
			"X": x,
		},
	)
}

type myOutputFunc struct {
}

func (t *myOutputFunc) Output(r types.Record) {
	fmt.Println(r)
}

type myAssignTimeWithPuncatuatedWatermark struct {
}

func (t *myAssignTimeWithPuncatuatedWatermark) ExtractTimestamp(r types.Record, pts int64) int64 {
	return cast.ToInt64(r.Get("T"))
}

func (t *myAssignTimeWithPuncatuatedWatermark) GetNextWatermark(r types.Record, ets int64) (wm *types.Watermark) {
	isWatermark := cast.ToBool(r.Get("Watermark"))
	if isWatermark {
		return types.NewWatermark(time.Unix(ets, 0))
	}
	return
}

func main() {
	input1 := make(chan map[string]interface{})
	input2 := make(chan map[string]interface{})
	flow, source := stream.New("tumbling_event_window")
	outfunc := &myOutputFunc{}
	source.MapChannels(input1, input2).
		AssignTimeWithPuncatuatedWatermark(&myAssignTimeWithPuncatuatedWatermark{}).
		Map(&myMapFunc{}).
		KeyBy("D1", "D2").
		TimeWindow(2).
		Reduce(&myReduceFunc{}).
		Output(outfunc)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, m := range dataset1 {
			input1 <- m
		}
		close(input1)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, m := range dataset2 {
			input2 <- m
		}
		close(input2)
	}()
	flow.Run()
	wg.Wait()
}

var (
	dataset1 = []map[string]interface{}{
		map[string]interface{}{
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:00").Unix(),
			"D1":        "A",
			"D2":        "A",
			"X":         1,
		},
		map[string]interface{}{
			"T":  utils.ParseTime("2018-10-15 18:00:01").Unix(),
			"D1": "A",
			"D2": "A",
			"X":  1,
		},
		map[string]interface{}{
			"T":  utils.ParseTime("2018-10-15 18:00:00").Unix(),
			"D1": "A",
			"D2": "B",
			"X":  1,
		},
		map[string]interface{}{
			"T":  utils.ParseTime("2018-10-15 18:00:01").Unix(),
			"D1": "B",
			"D2": "B",
			"X":  1,
		},
		map[string]interface{}{
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:02").Unix(),
			"D1":        "A",
			"D2":        "B",
			"X":         1,
		},
		map[string]interface{}{
			"T":  utils.ParseTime("2018-10-15 18:00:02").Unix(),
			"D1": "A",
			"D2": "B",
			"X":  1,
		},
		map[string]interface{}{
			"T":  utils.ParseTime("2018-10-15 18:00:02").Unix(),
			"D1": "A",
			"D2": "A",
			"X":  1,
		},
		map[string]interface{}{
			"T":  utils.ParseTime("2018-10-15 18:00:03").Unix(),
			"D1": "A",
			"D2": "A",
			"X":  1,
		},
		map[string]interface{}{
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:04").Unix(),
			"D1":        "C",
			"D2":        "C",
			"X":         1,
		},
	}

	dataset2 = []map[string]interface{}{
		map[string]interface{}{
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:00").Unix(),
			"D1":        "B",
			"D2":        "B",
			"X":         1,
		},
		map[string]interface{}{
			"T":  utils.ParseTime("2018-10-15 18:00:01").Unix(),
			"D1": "A",
			"D2": "B",
			"X":  1,
		},
		map[string]interface{}{
			"T":  utils.ParseTime("2018-10-15 18:00:00").Unix(),
			"D1": "A",
			"D2": "B",
			"X":  1,
		},
		map[string]interface{}{
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:01").Unix(),
			"D1":        "B",
			"D2":        "B",
			"X":         1,
		},
		map[string]interface{}{
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:02").Unix(),
			"D1":        "A",
			"D2":        "A",
			"X":         1,
		},
		map[string]interface{}{
			"T":  utils.ParseTime("2018-10-15 18:00:02").Unix(),
			"D1": "A",
			"D2": "B",
			"X":  1,
		},
		map[string]interface{}{
			"T":  utils.ParseTime("2018-10-15 18:00:02").Unix(),
			"D1": "B",
			"D2": "B",
			"X":  1,
		},
		map[string]interface{}{
			"T":  utils.ParseTime("2018-10-15 18:00:03").Unix(),
			"D1": "B",
			"D2": "A",
			"X":  1,
		},
		map[string]interface{}{
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:04").Unix(),
			"D1":        "C",
			"D2":        "C",
			"X":         1,
		},
	}
)
