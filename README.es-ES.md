
</think>

# wstream
[![Build Status](https://travis-ci.com/zhnpeng/wstream.svg?branch=master)](https://travis-ci.com/zhnpeng/wstream)
[![codecov](https://codecov.io/gh/zhnpeng/wstream/branch/master/graph/badge.svg)](https://codecov.io/gh/zhnpeng/wstream)

API de procesamiento en streaming para Go

## Instalación
``` console
go get github.com/zhnpeng/wstream
```

## Operadores admitidos

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

## Características

* El operador downstream recibe una copia de cada registro del upstream, evitando condiciones de carrera (data races).

## Uso

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

## Ejemplos

### Reduce con ventana de tiempo de eventos tumbling

[examples/windows/tumbling_time_window.go](examples/windows/tumbling_time_window.go)

## Por hacer

* [ ] multiplexor local para conectar/unir múltiples streams/flujos de forma desacoplada
* [ ] soporte para multiplexor remoto con protocolo de transporte personalizado
