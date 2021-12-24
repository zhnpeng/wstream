package coordinator

import (
	"bytes"
	"github.com/sirupsen/logrus"
	"github.com/tinylib/msgp/msgp"
	"github.com/zhnpeng/wstream/multiplexer/common"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

var socketConsumerCoordinators = sync.Map{}
var socketProducerCoordinators = sync.Map{}

func GetOrCreateSocketCoordinatorConsumer(id string, params SocketConsumerParams) common.MessageQueue {
	coord, _ := socketConsumerCoordinators.LoadOrStore(id, NewSocketConsumerCoordinator(id, params))
	return coord.(*SocketConsumerCoordinator).input
}

func GetOrCreateSocketCoordinatorProducer(id string, params SocketProducerParams) common.MessageQueue {
	coord, _ := socketProducerCoordinators.LoadOrStore(id, NewSocketProducerCoordinator(id, params))
	return coord.(*SocketProducerCoordinator).output
}

type SocketConsumerParams struct {
	Network string
	Address string
	ReconnectInterval time.Duration
	FileMode os.FileMode
}

type SocketConsumerCoordinator struct {
	id string

	network string
	address string
	reconnectInterval time.Duration
	fileMode os.FileMode
	logger logrus.Logger

	input    common.MessageQueue
	listener io.Closer
	mutex             sync.Mutex
}

func NewSocketConsumerCoordinator(id string, params SocketConsumerParams) *SocketConsumerCoordinator {
	queue := make(common.MessageQueue)
	corrd := &SocketConsumerCoordinator{
		id: id,
		input: queue,
		mutex: sync.Mutex{},

		network: params.Network,
		address: params.Address,
		reconnectInterval: params.ReconnectInterval,
		fileMode: params.FileMode,
	}
	go corrd.run()
	return corrd
}

func (c *SocketConsumerCoordinator) run() {
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

func (c *SocketConsumerCoordinator) handleMessageFromConnection(conn net.Conn) {
	for {
		var msg common.Message
		err := msgp.Decode(conn, &msg)
		if err != nil {
			c.logger.Warning("Failed to decode msg")
			continue
		}
		c.input <- msg
	}
}

func (c *SocketConsumerCoordinator) closeListener() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.listener == nil {
		return
	}
	c.logger.Debugf("Closing socket %s", c.address)
	_ = c.listener.Close()
	c.listener = nil
}

func (c *SocketConsumerCoordinator) tryRemoveUnixSocket() {
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

type SocketProducerParams struct {
	BatchSize  int
	BatchCount int

	Network string
	Address string
	Timeout time.Duration
}

type SocketProducerCoordinator struct {
	id string

	output common.MessageQueue

	conn net.Conn

	buffer     bytes.Buffer
	batchSize  int
	batchCount int

	network string
	address string
	timeout time.Duration
}

func NewSocketProducerCoordinator(id string, params SocketProducerParams) *SocketProducerCoordinator {
	queue := make(common.MessageQueue)

	corrd := &SocketProducerCoordinator{
		id: id,
		output: queue,

		batchSize: params.BatchSize,
		batchCount: params.BatchCount,
		network: params.Network,
		address: params.Address,
		timeout: params.Timeout,
	}
	go corrd.run()
	return corrd
}

func (c *SocketProducerCoordinator) run() {
	for {
		select {
		case msg, ok := <- c.output:
			if !ok {
				_ = c.closeConnection()
				return
			}
			c.onMessage(msg)
		}
	}
}

func (c *SocketProducerCoordinator) tryConnect() error {
	if c.conn != nil {
		return nil
	}

	conn, err := net.DialTimeout(c.network, c.address, c.timeout)
	if err != nil {
		return err
	}

	c.conn = conn
	return nil
}

func (c *SocketProducerCoordinator) closeConnection() error {
		if c.conn != nil {
		// ignore any error
		_ = c.conn.Close()
		c.conn = nil
	}
	return nil
}

func (c *SocketProducerCoordinator) writeMessage(msg common.Message) error {
	err := msgp.Encode(&c.buffer, &msg)
	if err != nil {
		return err
	}
	c.batchCount++
	if c.batchCount >= c.batchSize {
		_, err := c.conn.Write(c.buffer.Bytes())
		c.buffer = bytes.Buffer{}
		c.batchCount = 0
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *SocketProducerCoordinator) onMessage(msg common.Message) {
	if c.tryConnect() != nil {
		err := c.writeMessage(msg)
		if err != nil {
			logrus.Error(err)
		}
	}
}