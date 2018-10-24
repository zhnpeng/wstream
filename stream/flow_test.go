package stream

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
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

type testDebugFunc struct {
	Mu      sync.Mutex
	Records []types.Record
}

func (t *testDebugFunc) Debug(r types.Record) {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	t.Records = append(t.Records, r)
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
	outfunc := &testDebugFunc{}
	source.MapChannels(input1, input2).
		TimestampWithPuncatuatedWatermark(&testTimestampWithPuncatuatedWatermark{}).
		Map(&testMapFunc{}).
		KeyBy("D1", "D2").
		TimeWindow(2).
		Reduce(&testReduceFunc{}).
		Debug(outfunc)
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

	got := outfunc.Records
	if len(got) != 7 {
		t.Errorf("length of got records not right got %d, want %d", len(outfunc.Records), 7)
	}
	sort.SliceStable(got, func(i, j int) bool {
		ri := got[i]
		rj := got[j]
		if ri.Time().Before(rj.Time()) {
			return true
		} else if ri.Time().Equal(rj.Time()) {
			ki := cast.ToString(ri.Key()[0]) + cast.ToString(ri.Key()[1])
			kj := cast.ToString(rj.Key()[0]) + cast.ToString(rj.Key()[1])
			return ki < kj
		}
		return false
	})
	want := []types.Record{
		&types.MapRecord{
			T: utils.ParseTime("2018-10-15 18:00:00"),
			K: []interface{}{"A", "A"},
			V: map[string]interface{}{
				"X": 4,
			},
		},
		&types.MapRecord{
			T: utils.ParseTime("2018-10-15 18:00:00"),
			K: []interface{}{"A", "B"},
			V: map[string]interface{}{
				"X": 6,
			},
		},
		&types.MapRecord{
			T: utils.ParseTime("2018-10-15 18:00:00"),
			K: []interface{}{"B", "B"},
			V: map[string]interface{}{
				"X": 6,
			},
		},
		&types.MapRecord{
			T: utils.ParseTime("2018-10-15 18:00:02"),
			K: []interface{}{"A", "A"},
			V: map[string]interface{}{
				"X": 6,
			},
		},
		&types.MapRecord{
			T: utils.ParseTime("2018-10-15 18:00:02"),
			K: []interface{}{"A", "B"},
			V: map[string]interface{}{
				"X": 6,
			},
		},
		&types.MapRecord{
			T: utils.ParseTime("2018-10-15 18:00:02"),
			K: []interface{}{"B", "A"},
			V: map[string]interface{}{
				"X": 2,
			},
		},
		&types.MapRecord{
			T: utils.ParseTime("2018-10-15 18:00:02"),
			K: []interface{}{"B", "B"},
			V: map[string]interface{}{
				"X": 2,
			},
		},
	}
	if diff := cmp.Diff(got, want); diff != "" {
		for _, r := range got {
			fmt.Println(r)
		}
		t.Error(diff)
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
