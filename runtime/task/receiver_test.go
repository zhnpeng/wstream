package task

import (
	"testing"

	"github.com/wandouz/wstream/types"
	"github.com/wandouz/wstream/utils"
)

func TestReceiver_Run(t *testing.T) {
	recv := &Receiver{}
	input1 := make(types.ItemChan)
	input2 := make(types.ItemChan)
	input3 := make(types.ItemChan)
	recv.AddInput(input1)
	recv.AddInput(input2)
	recv.AddInput(input3)

	item1 := []types.Item{
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:00:00"),
			nil,
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:00:11"),
			nil,
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:00:13"),
			nil,
		),
		types.NewWaterMark(
			utils.TimeParse("2018-08-05 21:00:05"),
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:00:14"),
			nil,
		),
		types.NewWaterMark(
			utils.TimeParse("2018-08-05 21:00:10"),
		),
	}
	item2 := []types.Item{
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:00:00"),
			nil,
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:00:11"),
			nil,
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:00:13"),
			nil,
		),
		types.NewWaterMark(
			utils.TimeParse("2018-08-05 21:00:05"),
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:00:14"),
			nil,
		),
	}
	item3 := []types.Item{
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:00:00"),
			nil,
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:00:11"),
			nil,
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:00:13"),
			nil,
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:00:14"),
			nil,
		),
		types.NewWaterMark(
			utils.TimeParse("2018-08-05 21:00:10"),
		),
		types.NewMapRecord(
			utils.TimeParse("2018-08-05 21:00:15"),
			nil,
		),
	}
	recv.Run()
}
