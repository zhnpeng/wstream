package stream

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
	"github.com/zhnpeng/wstream/types"
	"github.com/zhnpeng/wstream/utils"
)

var debug = true

func TestFlow_Tumbling_Time_Window(t *testing.T) {
	input1 := make(chan map[string]interface{})
	input2 := make(chan map[string]interface{})
	flow, source := New("test")
	outfunc := &testDebug{}
	source.MapChannels(input1, input2).
		AssignTimeWithPuncatuatedWatermark(&testAssignTimeWithPuncatuatedWatermark{}).
		Map(&testMapPlusOne{}).
		KeyBy("D1", "D2").
		TimeWindow(2).
		Reduce(&testWindowReduce{}).
		Debug(outfunc)

	if debug {
		flow.LocalTransform()
		fmt.Printf("%T, %+v\n", flow.GetStream(0), flow.GetStream(0))
		fmt.Printf("%T, %+v\n", flow.GetTask(0), flow.GetTask(0))
		fmt.Printf("%+v\n", flow.Graph)
		fmt.Println("++++++++++++++++++")
		flow.BFSBoth(0, func(v, w int, c int64) {
			fmt.Printf("%T, %+v\n", flow.GetStream(w), flow.GetStream(w))
			fmt.Printf("%T, %+v\n", flow.GetTask(w), flow.GetTask(w))
		})
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, m := range dataset1 {
			if debug {
				logrus.Debugf("input1 <- %v", m)
			}
			input1 <- m
		}
		close(input1)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, m := range dataset2 {
			if debug {
				logrus.Debugf("input2 <- %v", m)
			}
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

func TestFlow_Multiplex_Rolling_Reduce(t *testing.T) {
	input1 := make(chan map[string]interface{})
	input2 := make(chan map[string]interface{})
	flow, source := New("multiplex_rolling_reduce")
	outfunc1 := &testDebug{}
	outfunc2 := &testDebug{}
	src := source.MapChannels(input1, input2)

	src.Map(&testMapSetOne{}).
		KeyBy("D1", "D2").
		Reduce(&testReduce{}).
		Debug(outfunc1)

	src.Map(&testMapSetOne{}).
		KeyBy("D1").
		Reduce(&testReduce{}).
		Debug(outfunc2)

	// var buf bytes.Buffer
	// err := gob.NewEncoder(&buf).Encode(flow)
	// require.Nil(t, err)
	// decoder := gob.NewDecoder(bytes.NewReader(buf.Bytes()))
	// var nf Flow
	// err = decoder.Decode(&nf)
	// require.Nil(t, err)

	if debug {
		// Debug
		flow.LocalTransform()
		fmt.Printf("%T, %+v\n", flow.GetStream(0), flow.GetStream(0))
		fmt.Printf("%T, %+v\n", flow.GetTask(0), flow.GetTask(0))
		flow.BFSBoth(0, func(v, w int, c int64) {
			fmt.Printf("%T, %+v\n", flow.GetStream(w), flow.GetStream(w))
			fmt.Printf("%T, %+v\n", flow.GetTask(w), flow.GetTask(w))
		})
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, m := range dataset3 {
			input1 <- m
		}
		close(input1)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, m := range dataset4 {
			input2 <- m
		}
		close(input2)
	}()
	flow.Run()
	wg.Wait()
	sort.Slice(outfunc1.Records, func(i, j int) bool {
		ri := outfunc1.Records[i]
		rj := outfunc1.Records[j]
		ki := fmt.Sprintf("%v%d", ri.Key(), ri.Get("X"))
		kj := fmt.Sprintf("%v%d", rj.Key(), rj.Get("X"))
		return ki < kj
	})
	sort.Slice(outfunc2.Records, func(i, j int) bool {
		ri := outfunc2.Records[i]
		rj := outfunc2.Records[j]
		ki := fmt.Sprintf("%v%d", ri.Key(), ri.Get("X"))
		kj := fmt.Sprintf("%v%d", rj.Key(), rj.Get("X"))
		return ki < kj
	})
	want1 := []types.Record{
		&types.MapRecord{
			K: []interface{}{"A", "A"},
			V: map[string]interface{}{
				"X": 1,
			},
		},
		&types.MapRecord{
			K: []interface{}{"A", "B"},
			V: map[string]interface{}{
				"X": 1,
			},
		},
		&types.MapRecord{
			K: []interface{}{"A", "B"},
			V: map[string]interface{}{
				"X": 2,
			},
		},
		&types.MapRecord{
			K: []interface{}{"B", "B"},
			V: map[string]interface{}{
				"X": 1,
			},
		},
		&types.MapRecord{
			K: []interface{}{"B", "B"},
			V: map[string]interface{}{
				"X": 2,
			},
		},
		&types.MapRecord{
			K: []interface{}{"B", "C"},
			V: map[string]interface{}{
				"X": 1,
			},
		},
		&types.MapRecord{
			K: []interface{}{"C", "C"},
			V: map[string]interface{}{
				"X": 1,
			},
		},
		&types.MapRecord{
			K: []interface{}{"C", "C"},
			V: map[string]interface{}{
				"X": 2,
			},
		},
	}
	want2 := []types.Record{
		&types.MapRecord{
			K: []interface{}{"A"},
			V: map[string]interface{}{
				"X": 1,
			},
		},
		&types.MapRecord{
			K: []interface{}{"A"},
			V: map[string]interface{}{
				"X": 2,
			},
		},
		&types.MapRecord{
			K: []interface{}{"A"},
			V: map[string]interface{}{
				"X": 3,
			},
		},
		&types.MapRecord{
			K: []interface{}{"B"},
			V: map[string]interface{}{
				"X": 1,
			},
		},
		&types.MapRecord{
			K: []interface{}{"B"},
			V: map[string]interface{}{
				"X": 2,
			},
		},
		&types.MapRecord{
			K: []interface{}{"B"},
			V: map[string]interface{}{
				"X": 3,
			},
		},
		&types.MapRecord{
			K: []interface{}{"C"},
			V: map[string]interface{}{
				"X": 1,
			},
		},
		&types.MapRecord{
			K: []interface{}{"C"},
			V: map[string]interface{}{
				"X": 2,
			},
		},
	}
	if diff := cmp.Diff(outfunc1.Records, want1); diff != "" {
		t.Fatal(diff)
	}
	if diff := cmp.Diff(outfunc2.Records, want2); diff != "" {
		t.Fatal(diff)
	}
}

// Functions
type testMapSetOne struct{}

func (tmf *testMapSetOne) Map(r types.Record) (o types.Record) {
	_ = r.Set("X", 1)
	return r
}

type testMapPlusOne struct{}

func (tmf *testMapPlusOne) Map(r types.Record) (o types.Record) {
	x := cast.ToInt(r.Get("X"))
	r.Set("X", x+1)
	return r
}

type testWindowReduce struct{}

func (trf *testWindowReduce) Accmulater(window windows.Window, a types.Record) types.Record {
	acc := types.NewMapRecord(
		window.Start(),
		map[string]interface{}{
			"X": cast.ToInt(a.Get("X")),
		},
	)
	acc.SetKey(a.Key())
	return acc
}

func (trf *testWindowReduce) Reduce(a, b types.Record) types.Record {
	a.Set("X", cast.ToInt(a.Get("X"))+cast.ToInt(b.Get("X")))
	return a
}

type testReduce struct{}

func (trf *testReduce) Accmulater(a types.Record) types.Record {
	return types.NewRawMapRecord(
		map[string]interface{}{
			"X": cast.ToInt(a.Get("X")),
		},
	)
}

func (trf *testReduce) Reduce(a, b types.Record) types.Record {
	x := cast.ToInt(a.Get("X")) + cast.ToInt(b.Get("X"))
	return types.NewRawMapRecord(
		map[string]interface{}{
			"X": x,
		},
	)
}

type testDebug struct {
	Mu      sync.Mutex
	Records []types.Record
}

func (t *testDebug) Debug(r types.Record) {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	t.Records = append(t.Records, r)
}

type testAssignTimeWithPuncatuatedWatermark struct {
}

func (t *testAssignTimeWithPuncatuatedWatermark) ExtractTimestamp(r types.Record, pts int64) int64 {
	return cast.ToInt64(r.Get("T"))
}

func (t *testAssignTimeWithPuncatuatedWatermark) GetNextWatermark(r types.Record, ets int64) (wm *types.Watermark) {
	isWatermark := cast.ToBool(r.Get("Watermark"))
	if isWatermark {
		return types.NewWatermark(time.Unix(ets, 0))
	}
	return
}

// Data
var (
	dataset1 = []map[string]interface{}{
		{
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:00").Unix(),
			"D1":        "A",
			"D2":        "A",
			"X":         1,
		},
		{
			"T":  utils.ParseTime("2018-10-15 18:00:01").Unix(),
			"D1": "A",
			"D2": "A",
			"X":  1,
		},
		{
			"T":  utils.ParseTime("2018-10-15 18:00:00").Unix(),
			"D1": "A",
			"D2": "B",
			"X":  1,
		},
		{
			"T":  utils.ParseTime("2018-10-15 18:00:01").Unix(),
			"D1": "B",
			"D2": "B",
			"X":  1,
		},
		{
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:02").Unix(),
			"D1":        "A",
			"D2":        "B",
			"X":         1,
		},
		{
			"T":  utils.ParseTime("2018-10-15 18:00:02").Unix(),
			"D1": "A",
			"D2": "B",
			"X":  1,
		},
		{
			"T":  utils.ParseTime("2018-10-15 18:00:02").Unix(),
			"D1": "A",
			"D2": "A",
			"X":  1,
		},
		{
			"T":  utils.ParseTime("2018-10-15 18:00:03").Unix(),
			"D1": "A",
			"D2": "A",
			"X":  1,
		},
		{
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:04").Unix(),
			"D1":        "C",
			"D2":        "C",
			"X":         1,
		},
	}

	dataset2 = []map[string]interface{}{
		{
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:00").Unix(),
			"D1":        "B",
			"D2":        "B",
			"X":         1,
		},
		{
			"T":  utils.ParseTime("2018-10-15 18:00:01").Unix(),
			"D1": "A",
			"D2": "B",
			"X":  1,
		},
		{
			"T":  utils.ParseTime("2018-10-15 18:00:00").Unix(),
			"D1": "A",
			"D2": "B",
			"X":  1,
		},
		{
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:01").Unix(),
			"D1":        "B",
			"D2":        "B",
			"X":         1,
		},
		{
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:02").Unix(),
			"D1":        "A",
			"D2":        "A",
			"X":         1,
		},
		{
			"T":  utils.ParseTime("2018-10-15 18:00:02").Unix(),
			"D1": "A",
			"D2": "B",
			"X":  1,
		},
		{
			"T":  utils.ParseTime("2018-10-15 18:00:02").Unix(),
			"D1": "B",
			"D2": "B",
			"X":  1,
		},
		{
			"T":  utils.ParseTime("2018-10-15 18:00:03").Unix(),
			"D1": "B",
			"D2": "A",
			"X":  1,
		},
		{
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:04").Unix(),
			"D1":        "C",
			"D2":        "C",
			"X":         1,
		},
	}
	dataset3 = []map[string]interface{}{
		{
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:00").Unix(),
			"D1":        "A",
			"D2":        "A",
			"X":         1,
		},

		{
			"T":  utils.ParseTime("2018-10-15 18:00:00").Unix(),
			"D1": "A",
			"D2": "B",
			"X":  1,
		},
		{
			"T":  utils.ParseTime("2018-10-15 18:00:01").Unix(),
			"D1": "B",
			"D2": "B",
			"X":  1,
		},
		{
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:04").Unix(),
			"D1":        "C",
			"D2":        "C",
			"X":         1,
		},
	}

	dataset4 = []map[string]interface{}{
		{
			"T":  utils.ParseTime("2018-10-15 18:00:02").Unix(),
			"D1": "A",
			"D2": "B",
			"X":  1,
		},
		{
			"T":  utils.ParseTime("2018-10-15 18:00:02").Unix(),
			"D1": "B",
			"D2": "B",
			"X":  1,
		},
		{
			"T":  utils.ParseTime("2018-10-15 18:00:03").Unix(),
			"D1": "B",
			"D2": "C",
			"X":  1,
		},
		{
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:04").Unix(),
			"D1":        "C",
			"D2":        "C",
			"X":         1,
		},
	}
)
