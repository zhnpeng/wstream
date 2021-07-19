package producer

import (
	"bytes"
	"context"
	"net"
	"time"

	"github.com/tinylib/msgp/msgp"

	"github.com/sirupsen/logrus"

	"github.com/zhnpeng/wstream/multiplexer"
)

type Socket struct {
	*BasicProducer
	conn net.Conn

	buffer     bytes.Buffer
	batchSize  int
	batchCount int

	network string
	address string
	timeout time.Duration
}

func (p *Socket) tryConnect() error {
	if p.conn != nil {
		return nil
	}

	conn, err := net.DialTimeout(p.network, p.address, p.timeout)
	if err != nil {
		return err
	}

	p.conn = conn
	return nil
}

func (p *Socket) closeConnection() error {
	if p.conn != nil {
		// ignore any error
		_ = p.conn.Close()
		p.conn = nil
	}
	return nil
}

func (p *Socket) Produce(ctx context.Context) {
	p.Initialized()
	go p.controlLoop(ctx)
	p.messageLoop(p.onMessage)
}

func (p *Socket) writeMessage(msg multiplexer.Message) error {
	err := msgp.Encode(&p.buffer, &msg)
	if err != nil {
		return err
	}
	p.batchCount++
	if p.batchCount >= p.batchSize {
		_, err := p.conn.Write(p.buffer.Bytes())
		p.buffer = bytes.Buffer{}
		p.batchCount = 0
		if err != nil {
			return err
		}
	}

}

func (p *Socket) onMessage(msg multiplexer.Message) {
	if p.tryConnect() != nil {
		err := p.writeMessage(msg)
		if err != nil {
			logrus.Error(err)
		}
	}
}
