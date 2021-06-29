# wstream
[![Build Status](https://travis-ci.org/zhnpeng/wstream.svg?branch=master)](https://travis-ci.org/zhnpeng/wstream)
[![codecov](https://codecov.io/gh/zhnpeng/wstream/branch/master/graph/badge.svg)](https://codecov.io/gh/zhnpeng/wstream)

streaming processing API for golang

## Install
``` console
go get github.com/zhnpeng/wstream
```

## Supported Operator

* Map
* FlatMap
* Filter
* Reduce
* KeyBy
* TimeWindow
* SlidingTimeWindow
* CountWindow
* SlidingCountWindow
* Window.Reduce
* Window.Apply

## Characteristics

* downstream operator receive a clone of each record from upstream avoiding data race.

## Usage

```go
func main() {
    flow, source := stream.New("tumbling_event_window")
    source.MapChannels(input1, input2). //two map[string]interface{} channels as input
        AssignTimeWithPuncatuatedWatermark( // assign time and generate watermark
            &myAssignTimeWithPuncatuatedWatermark{},
        ).
        Map(&myMapFunc{}).
        KeyBy("Key1", "Key2"). // group by "Key1" and "Key2"
        TimeWindow(2). // tumbling event time window for every 2 seconds
        Reduce(&myReduceFunc{}). // reduce on all datas for each window
        Output(outfunc)
    flow.Run() // Run infinity until input1 and input2 are closed
}
```

## Examples

### Reduce with tumbling event time window

[examples/windows/tumbling_time_window.go](examples/windows/tumbling_time_window.go)

## TODO

* [ ] local multiplexer to connect/join multiple stream/flow in decoupling way
* [ ] remote multiplexer support custom transport protocol