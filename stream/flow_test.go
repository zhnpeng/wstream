package stream

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/spf13/cast"
	"github.com/wandouz/wstream/types"
	"github.com/wandouz/wstream/utils"
)

type testMapFunc struct{}

func (tmf *testMapFunc) Map(r types.Record) (o types.Record) {
	x := cast.ToInt(r.Get("X"))
	r.Set("X", x+1)
	return r
}

type testReduceFunc struct{}

func (trf *testReduceFunc) Reduce(a, b types.Record) types.Record {
	if a == nil {
		return types.NewRawMapRecord(
			map[string]interface{}{
				"X": cast.ToInt(b.Get("X")),
			},
		)
	}
	x := cast.ToInt(a.Get("X")) + cast.ToInt(b.Get("X"))
	return types.NewRawMapRecord(
		map[string]interface{}{
			"X": x,
		},
	)
}

type testSlice struct {
	coll []types.Record
	mu   sync.Mutex
}

func (t *testSlice) Append(r types.Record) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.coll = append(t.coll, r)
}

func (t *testSlice) Records() []types.Record {
	return t.coll
}

type testDebugFunc struct {
	Records *testSlice
}

func (t *testDebugFunc) Debug(r types.Record) {
	if t.Records == nil {
		t.Records = &testSlice{}
	}
	t.Records.Append(r)
}

type testTimestampWithPuncatuatedWatermark struct {
}

func (t *testTimestampWithPuncatuatedWatermark) ExtractTimestamp(r types.Record, pts int64) int64 {
	return cast.ToInt64(r.Get("T"))
}

func (t *testTimestampWithPuncatuatedWatermark) GetNextWatermark(r types.Record, ets int64) (wm *types.Watermark) {
	isWatermark := cast.ToBool(r.Get("Watermark"))
	if isWatermark {
		return types.NewWatermark(time.Unix(ets, 0))
	}
	return
}

func TestFlow_Run(t *testing.T) {
	input1 := make(chan map[string]interface{})
	input2 := make(chan map[string]interface{})
	flow, source := New("test")
	Debug := &testDebugFunc{}
	source.MapChannels(input1, input2).
		TimestampWithPuncatuatedWatermark(&testTimestampWithPuncatuatedWatermark{}).
		Map(&testMapFunc{}).
		KeyBy("D1", "D2").
		TimeWindow(2).
		Reduce(&testReduceFunc{}).
		Debug(Debug)
	// Debug
	// flow.Transform()
	// fmt.Println(reflect.TypeOf(flow.GetStream(0)))
	// fmt.Println(flow.GetStream(0))
	// fmt.Println(flow.GetTask(0))
	// flow.BFSBoth(0, func(v, w int, c int64) {
	// 	fmt.Println(reflect.TypeOf(flow.GetStream(w)))
	// 	fmt.Println(flow.GetStream(w))
	// 	fmt.Println(flow.GetTask(w))
	// })
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

	fmt.Println(len(Debug.Records.Records()))
	for _, r := range Debug.Records.Records() {
		fmt.Println(r)
	}
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
