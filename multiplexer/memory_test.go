package multiplexer

import (
	"github.com/stretchr/testify/assert"
"sync"
	"testing"

	"github.com/zhnpeng/wstream/multiplexer/common"
	"github.com/zhnpeng/wstream/types"
)

func TestMemoryMultiplexer(t *testing.T) {
	producer := NewMemoryProducer("my_memory")
	consumer := NewMemoryConsumer("my_memory")

	messages := []int64{1, 2, 3, 4}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer producer.Close()
		for _, i := range messages {
			record := types.NewRawMapRecord(map[string]interface{}{
				"value": i,
			})
			producer.Sender <- common.FromMapRecord(record)
		}
	}()

	var got []int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case msg, ok := <- consumer.Receiver:
				if !ok {
					return
				}
				rec, _ := msg.Data.AsMapRecord()
				got = append(got, rec.Get("value").(int64))
			}
		}
	}()

	wg.Wait()
	assert.Equal(t, messages, got)
}
