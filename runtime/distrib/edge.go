package distrib

import (
	"fmt"
	"net"

	"github.com/zhnpeng/wstream/pkg/com"

	"github.com/zhnpeng/wstream/runtime/execution"

	"github.com/zhnpeng/wstream/types"
)

type DistribEdge struct {
	conn net.Conn
	ch   chan types.Item
}

func NewDistribEdge(conn net.Conn) *DistribEdge {
	return &DistribEdge{
		conn: conn,
		ch:   make(chan types.Item, 100),
	}
}

func (n *DistribEdge) In() execution.InEdge {
	go func() {
		defer com.CatchPanic()
		for {
			select {
			case <-n.ch:
				// channel is closed by execution node
				_ = n.conn.Close()
				return
			default:
			}
			var buf []byte
			_, err := n.conn.Read(buf)
			if err != nil {
				// TODO: reconnect
				continue
			}
			// TODO: decode bytes into types.Item
			n.ch <- nil
		}
	}()
	return n.ch
}

func (n *DistribEdge) Out() execution.OutEdge {
	go func() {
		defer com.CatchPanic()
		for {
			select {
			case item, ok := <-n.ch:
				if !ok {
					// channel is closed by execution node
					_ = n.conn.Close()
					return
				}
				var buf []byte
				// TODO: encode types.Item
				fmt.Println(item)
				_, err := n.conn.Write(buf)
				if err != nil {
					// TODO: reconnect
					continue
				}
			}
		}
	}()
	return n.ch
}
