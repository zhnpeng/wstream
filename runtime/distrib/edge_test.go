package distrib

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/zhnpeng/wstream/types"
)

var tSockAddr = "/tmp/test_edge_dist.sock"

func TestDistribEdge_In(t *testing.T) {
	listener, err := net.Listen("unix", tSockAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			conn, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}
			wg.Add(1)
			go func(conn net.Conn) {
				edge := NewDistribEdge(conn)
				in := edge.In()
				defer wg.Done()
				for {
					select {
					case <-stopCh:
						return
					case item := <-in:
						fmt.Println(item)
					}
				}
			}(conn)
		}
	}()

	conn, err := net.Dial("unix", tSockAddr)
	if err != nil {
		t.Fatal(err)
	}
	edge := NewDistribEdge(conn)
	out := edge.Out()
	for i := 0; i < 10; i++ {
		out <- types.NewMapRecord(time.Time{}, map[string]interface{}{"value": i})
	}
	close(stopCh)
	wg.Wait()
}
