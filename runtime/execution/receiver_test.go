package task

import (
	"sync"
	"testing"

	"github.com/wandouz/wstream/helpers"
	"github.com/wandouz/wstream/types"
)

func TestReceiver_Run_Multi_Way(t *testing.T) {
	recv := NewReceiver()
	input1 := make(Edge)
	input2 := make(Edge)
	input3 := make(Edge)
	recv.Add(input1.In())
	recv.Add(input2.In())
	recv.Add(input3.In())

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
			helpers.TimeParse("2018-08-05 21:05:00"),
			va,
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:01"),
			va,
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:02"),
			va,
		),
		types.NewWatermark(
			helpers.TimeParse("2018-08-05 21:05:00"),
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:04"),
			va,
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:06"),
			va,
		),
		types.NewWatermark(
			helpers.TimeParse("2018-08-05 21:05:03"),
		),
	}
	item2 := []types.Item{
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:03"),
			vb,
		),
		types.NewWatermark(
			helpers.TimeParse("2018-08-05 21:05:00"),
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:05"),
			vb,
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:07"),
			vb,
		),
		types.NewWatermark(
			helpers.TimeParse("2018-08-05 21:05:05"),
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:14"),
			vb,
		),
	}
	item3 := []types.Item{
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:11"),
			vc,
		),
		types.NewWatermark(
			helpers.TimeParse("2018-08-05 21:05:13"),
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:14"),
			vc,
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:15"),
			vc,
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:20"),
			vc,
		),
		types.NewWatermark(
			helpers.TimeParse("2018-08-05 21:05:10"),
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:25"),
			vc,
		),
		types.NewWatermark(
			helpers.TimeParse("2018-08-05 21:05:20"),
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
	var wg1 sync.WaitGroup
	wg1.Add(1)
	go func() {
		defer wg1.Done()
		go recv.Run()
		for {
			item, ok := <-recv.Next()
			if !ok {
				return
			}
			got = append(got, item)
		}
	}()

	wg.Wait()
	wg1.Wait()

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
			if i.Time() == helpers.TimeParse("2018-08-05 21:05:00") {
				idwm210500 = id
			} else if i.Time() == helpers.TimeParse("2018-08-05 21:05:03") {
				idwm210503 = id
			}
			mark.T = i.(*types.Watermark).Time()
		} else if i.Type() == types.TypeMapRecord {
			item := i.(*types.MapRecord)
			if item.V["mark"] == "A" {
				switch item.Time() {
				case helpers.TimeParse("2018-08-05 21:05:00"):
					id210500A = id
				case helpers.TimeParse("2018-08-05 21:05:01"):
					id210501A = id
				case helpers.TimeParse("2018-08-05 21:05:02"):
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

func TestReceiver_Run_Single_Way(t *testing.T) {
	recv := NewReceiver()
	input1 := make(Edge)
	recv.Add(input1.In())
	va := map[interface{}]interface{}{
		"mark": "A",
	}
	item1 := []types.Item{
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:00"),
			va,
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:01"),
			va,
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:02"),
			va,
		),
		types.NewWatermark(
			helpers.TimeParse("2018-08-05 21:05:00"),
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:04"),
			va,
		),
		types.NewMapRecord(
			helpers.TimeParse("2018-08-05 21:05:06"),
			va,
		),
		types.NewWatermark(
			helpers.TimeParse("2018-08-05 21:05:03"),
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

	got := make([]types.Item, 0)
	var wg1 sync.WaitGroup
	wg1.Add(1)
	go func() {
		defer wg1.Done()
		go recv.Run()
		for {
			item, ok := <-recv.Next()
			if !ok {
				return
			}
			got = append(got, item)
		}
	}()

	wg.Wait()
	wg1.Wait()

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
			if i.Time() == helpers.TimeParse("2018-08-05 21:05:00") {
				idwm210500 = id
			} else if i.Time() == helpers.TimeParse("2018-08-05 21:05:03") {
				idwm210503 = id
			}
			mark.T = i.(*types.Watermark).Time()
		} else if i.Type() == types.TypeMapRecord {
			item := i.(*types.MapRecord)
			if item.V["mark"] == "A" {
				switch item.Time() {
				case helpers.TimeParse("2018-08-05 21:05:00"):
					id210500A = id
				case helpers.TimeParse("2018-08-05 21:05:01"):
					id210501A = id
				case helpers.TimeParse("2018-08-05 21:05:02"):
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
