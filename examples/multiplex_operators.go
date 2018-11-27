package main

import (
	"fmt"
	"sync"

	"github.com/spf13/cast"
	"github.com/wandouz/wstream/stream"
	"github.com/wandouz/wstream/types"
	"github.com/wandouz/wstream/utils"
)

type myMapFunc struct{}

func (tmf *myMapFunc) Map(r types.Record) (o types.Record) {
	_ = r.Set("Y", 1)
	return r
}

type myReduceFuncX struct{}

func (trf *myReduceFuncX) Accmulater(a types.Record) types.Record {
	return types.NewRawMapRecord(
		map[string]interface{}{
			"X": cast.ToInt(a.Get("X")),
		},
	)
}

func (trf *myReduceFuncX) Reduce(a, b types.Record) types.Record {
	x := cast.ToInt(a.Get("X")) + cast.ToInt(b.Get("X"))
	return types.NewRawMapRecord(
		map[string]interface{}{
			"X": x,
		},
	)
}

type myReduceFuncY struct{}

func (trf *myReduceFuncY) Accmulater(a types.Record) types.Record {
	return types.NewRawMapRecord(
		map[string]interface{}{
			"Y": cast.ToInt(a.Get("X")),
		},
	)
}

func (trf *myReduceFuncY) Reduce(a, b types.Record) types.Record {
	y := cast.ToInt(a.Get("Y")) + cast.ToInt(b.Get("X"))
	return types.NewRawMapRecord(
		map[string]interface{}{
			"Y": y,
		},
	)
}

type myOutputFunc struct {
}

func (t *myOutputFunc) Output(r types.Record) {
	fmt.Println(r)
}

func main() {
	input1 := make(chan map[string]interface{})
	input2 := make(chan map[string]interface{})
	flow, source := stream.New("multiplex_rolling_reduce")
	outfunc := &myOutputFunc{}
	src := source.MapChannels(input1, input2)

	src.Map(&myMapFunc{}).
		KeyBy("D1", "D2").
		Reduce(&myReduceFuncX{}).
		Output(outfunc)

	src.Map(&myMapFunc{}).
		KeyBy("D1").
		Reduce(&myReduceFuncY{}).
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
			"Watermark": true,
			"T":         utils.ParseTime("2018-10-15 18:00:01").Unix(),
			"D1":        "B",
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
