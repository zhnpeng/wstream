package producer

import (
	"context"
	"fmt"
	"testing"

	"github.com/spf13/cast"

	"github.com/zhnpeng/wstream/multiplexer"
	"github.com/zhnpeng/wstream/types"
	"gopkg.in/tomb.v1"
)

func TestMemory_Produce_Linkage(t *testing.T) {
	output := make(multiplexer.MessageQueue, 11)
	p := &Memory{
		BasicProducer: &BasicProducer{
			Tomb:     &tomb.Tomb{},
			messages: make(chan multiplexer.Message, 10),
		},
		output: output,
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	go p.Produce(ctx)

	for i := 0; i < 10; i++ {
		p.Write(multiplexer.Message{
			Data: types.NewRawMapRecord(map[string]interface{}{
				"value": i,
			}),
		})

		if msg, ok := <-output; ok {
			got := cast.ToInt(msg.Data.(types.Record).Get("value"))
			fmt.Println(got)
			if got != i {
				t.Errorf("Got: %d, Want %d", got, i)
			}
		} else {
			t.Fatal("unexpected chanel status")
		}
	}

	cancelFunc()
	err := p.Wait()
	if err != nil {
		t.Error(err)
	}
}
