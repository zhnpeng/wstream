package wio

import (
	zmq "github.com/pebbe/zmq4"
)

//RemotePipe is net pipe with zmq libs
type RemotePipe struct {
	Reader     *RemotePipeReader
	Writer     *RemotePipeWriter
	SourceNode string
	TargetNode string
}

type RemotePipeReader struct {
	context *zmq.Context //same context as Writer
	socket  zmq.Socket
}

func (reader *RemotePipeReader) Read() (msg []byte, err error) {
	msgs, err := reader.socket.RecvMessageBytes(0)
	if err != nil {
		return msg, err
	}
	return msgs[0], nil
}

type RemotePipeWriter struct {
	context *zmq.Context
	socket  zmq.Socket
}

//Write message msgs should invoke of router info and item
func (writer *RemotePipeWriter) Write(data []byte) (total int, err error) {
	return writer.socket.SendMessageDontwait(data)
}
