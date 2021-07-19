package producer

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/spf13/cast"

	"github.com/zhnpeng/wstream/multiplexer"
	"github.com/zhnpeng/wstream/types"
)

func TestMemory_Produce_Linkage(t *testing.T) {
	output := make(multiplexer.MessageQueue, 11)
	p := &Memory{
		BasicProducer: NewBasicProducer(10),
		mutex:         &sync.Mutex{},
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	go p.Produce(ctx)
	p.Link(output)

	for i := 0; i < 10; i++ {
		p.Write(multiplexer.Message{
			Data: types.NewRawMapRecord(map[string]interface{}{
				"value": i,
			}).AsRow(),
		})

		if msg, ok := <-output; ok {
			record, err := msg.Data.AsMapRecord()
			if err != nil {
				t.Fatal(err)
			}
			got := cast.ToInt(record.Get("value"))
			fmt.Println(got)
			if got != i {
				t.Errorf("Got: %d, Want %d", got, i)
			}
		}
	}

	cancelFunc()
	err := p.Wait()
	if err != nil {
		t.Error(err)
	}
}
