package operator

import (
	"bytes"
	"encoding/gob"

	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/operator/windowing/assigners"
	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

type Window struct {
	assigner assigners.WindowAssinger
	trigger  triggers.Trigger
}

func NewWindow(assigner assigners.WindowAssinger, trigger triggers.Trigger) execution.Operator {
	return &Window{
		assigner: assigner,
		trigger:  trigger,
	}
}

func (w *Window) New() execution.Operator {
	codec := encodeFunction(w.assigner)
	reader := bytes.NewReader(codec)
	decoder := gob.NewDecoder(reader)
	var assigner assigners.WindowAssinger
	err := decoder.Decode(&assigner)
	if err != nil {
		panic(err)
	}

	codec = encodeFunction(w.trigger)
	reader = bytes.NewReader(codec)
	decoder = gob.NewDecoder(reader)
	var trigger triggers.Trigger
	err = decoder.Decode(&trigger)
	if err != nil {
		panic(err)
	}

	return NewWindow(assigner, trigger)
}

func (w *Window) handleRecord(record types.Record, out utils.Emitter) {
}

func (w *Window) handleWatermark(wm *types.Watermark, out utils.Emitter) {
}

func (w *Window) Run(in *execution.Receiver, out utils.Emitter) {
	consume(in, out, w)
}
