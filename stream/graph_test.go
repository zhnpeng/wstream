package stream

import (
	"sync"
	"testing"
	"time"

	"github.com/wandouz/wstream/types"
)

type reduceFuncForGraphTest struct {
	InitialTime time.Time
}

type mapFuncForGraphTest struct{}

func (tmf *mapFuncForGraphTest) Map(record types.Record) (o types.Record) {
	return record
}

func (trf *reduceFuncForGraphTest) InitialAccmulator() types.Record {
	return types.NewMapRecord(trf.InitialTime, nil)
}

func (trf *reduceFuncForGraphTest) Reduce(a, b types.Record) types.Record {
	return b
}

func TestStreamGraph_Run(t *testing.T) {
	input1 := make(chan types.Item)
	input2 := make(chan types.Item)
	source := NewSourceStream("source")
	gph := source.graph
	source.Channels(input1, input2).SetPartition(4).
		Map(&mapFuncForGraphTest{}).
		KeyBy("A", "B").
		Reduce(&reduceFuncForGraphTest{time.Now()}).
		Map(&mapFuncForGraphTest{})
	gph.Transform()
	// debug
	// gph.BFSBoth(0, func(v, w int, c int64) {
	// 	fmt.Println(reflect.TypeOf(gph.GetStream(w)))
	// 	fmt.Println(gph.GetTask(w))
	// })
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		gph.Run()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Second)
		close(input1)
		close(input2)
	}()
	wg.Wait()
}
