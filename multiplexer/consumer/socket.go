package consumer

import (
	"context"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/tinylib/msgp/msgp"

	"github.com/zhnpeng/wstream/multiplexer"

	"github.com/sirupsen/logrus"
)

type Socket struct {
	*BasicConsumer
	listener          io.Closer
	network           string
	address           string
	mutex             sync.Mutex
	reconnectInterval time.Duration
	fileMode          os.FileMode

	output multiplexer.MessageQueue

	logger logrus.Logger
}

func (c *Socket) messageLoop() {
	var (
		err      error
		listener net.Listener
	)

	for {
		// create a listener
		for c.listener == nil {
			listener, err = net.Listen(c.network, c.address)
			if err == nil && c.network == "unix" {
				err = os.Chmod(c.address, c.fileMode)
			}

			if err == nil {
				c.listener = listener
				c.logger.Infof("Listening to %s://%s", c.network, c.address)
				// listen success break
				break
			}

			c.logger.Errorf("Failed to listen to %s://%s", c.network, c.address)
			if c.network == "unix" {
				c.tryRemoveUnixSocket()
			}

			time.Sleep(c.reconnectInterval)
		}

		conn, err := listener.Accept()

		if err == nil {
			c.logger.Debugf("Accept new connection from %s for %s", conn.RemoteAddr(), c.address)
			go c.handleMessageFromConnection(conn)
			continue
		}

		c.logger.WithError(err).Errorf("Socket accept failed for %s", c.address)
		c.closeListener()
	}
}

func (c *Socket) closeListener() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.listener == nil {
		return
	}
	c.logger.Debugf("Closing socket %s", c.address)
	_ = c.listener.Close()
	c.listener = nil
}

func (c *Socket) handleMessageFromConnection(conn net.Conn) {
	for {
		select {
		case <-c.Dying():
			return
		case <-c.Dead():
			return
		default:
			var msg multiplexer.Message
			err := msgp.Decode(conn, &msg)
			if err != nil {
				c.logger.Warning("Failed to decode msg")
				continue
			}
			c.output <- msg
		}
	}
}

func (c *Socket) tryRemoveUnixSocket() {
	// Try to create the socket file to check if it exists
	socketFile, err := os.Create(c.address)
	if os.IsExist(err) {
		c.logger.Warningf("Found existing socket %s. Trying to remove it", c.address)
		if err := os.Remove(c.address); err != nil {
			c.logger.Errorf("Failed to remove %s", c.address)
			return
		}

		c.logger.Warningf("Removed %s", c.address)
		return
	}

	_ = socketFile.Close()
	if err := os.Remove(c.address); err != nil {
		c.logger.WithError(err).Errorf("Could not remove test file %s", c.address)
	}
}

func (c *Socket) Consume(ctx context.Context) {
	go c.messageLoop()
	c.ControlLoop(ctx)
}
