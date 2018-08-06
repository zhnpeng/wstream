package task

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wandouz/wstream/types"
	"github.com/wandouz/wstream/utils"
)

func TestReceiver_Run(t *testing.T) {
	recv := NewReceiver()
	input1 := make(types.ItemChan)
	input2 := make(types.ItemChan)
	input3 := make(types.ItemChan)
	recv.AddInput(input1)
	recv.AddInput(input2)
	recv.AddInput(input3)
	va := map[interface{}]interface{}{
		"mark": "A",
	}
	vb := map[interface{}]interface{}{
		"mark": "B",
	}
	vc := map[interface{}]interface{}{
		"mark": "C",
	}
	item1 := []types.Item{
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:05:00"),
			va,
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:05:01"),
			va,
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:05:02"),
			va,
		),
		types.NewWaterMark(
			utils.TimeParse("2018-08-05 21:05:00"),
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:05:04"),
			va,
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:05:06"),
			va,
		),
		types.NewWaterMark(
			utils.TimeParse("2018-08-05 21:05:03"),
		),
	}
	item2 := []types.Item{
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:05:03"),
			vb,
		),
		types.NewWaterMark(
			utils.TimeParse("2018-08-05 21:05:00"),
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:05:05"),
			vb,
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:05:07"),
			vb,
		),
		types.NewWaterMark(
			utils.TimeParse("2018-08-05 21:05:05"),
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:05:14"),
			vb,
		),
	}
	item3 := []types.Item{
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:05:11"),
			vc,
		),
		types.NewWaterMark(
			utils.TimeParse("2018-08-05 21:05:13"),
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:05:14"),
			vc,
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:05:15"),
			vc,
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:05:20"),
			vc,
		),
		types.NewWaterMark(
			utils.TimeParse("2018-08-05 21:05:10"),
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:05:25"),
			vc,
		),
		types.NewWaterMark(
			utils.TimeParse("2018-08-05 21:05:20"),
		),
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, i := range item1 {
			input1 <- i
		}
		close(input1)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, i := range item2 {
			input2 <- i
		}
		close(input2)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, i := range item3 {
			input3 <- i
		}
		close(input3)
	}()

	got := make([]types.Item, 0)
	ctx, cancel := context.WithCancel(context.Background())
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-recv.Next():
				if !ok {
					return
				}
				got = append(got, item)
			}
		}
	}(ctx)

	wg.Wait()
	recv.Wait()
	time.Sleep(2 * time.Second)
	cancel()

	var mark types.Watermark
	var id210500A int
	var id210501A int
	var id210502A int
	var idwm210500 int
	var idwm210503 int
	for id, i := range got {
		if i.Type() == types.TypeWatermark {
			if i.Time().Before(mark.Time()) {
				t.Errorf("got an mis order watermark %v, want watermark after %v", i, mark)
			}
			fmt.Println(i)
			if i.Time() == utils.TimeParse("2018-08-05 21:05:00") {
				idwm210500 = id
			} else if i.Time() == utils.TimeParse("2018-08-05 21:05:03") {
				idwm210503 = id
			}
			mark.T = i.(*types.Watermark).Time()
		} else if i.Type() == types.TypeMapRecord {
			item := i.(*types.MapRecord)
			if item.V["mark"] == "A" {
				switch item.Time() {
				case utils.TimeParse("2018-08-05 21:05:00"):
					id210500A = id
				case utils.TimeParse("2018-08-05 21:05:01"):
					id210501A = id
				case utils.TimeParse("2018-08-05 21:05:02"):
					id210502A = id
				}
			}
		} else {
			t.Errorf("got unexcepted item %v", i)
		}
	}
	if !(id210500A < id210501A &&
		id210501A < id210502A &&
		id210502A < idwm210500 &&
		idwm210500 < idwm210503) {
		t.Errorf("receiver got unexcepted order for items %v", []int{
			id210500A, id210501A, id210502A, idwm210500, idwm210503,
		})
	}
}
