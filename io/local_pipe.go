package io

import "io"

//LocalPipe is pipe in memory
type LocalPipe struct {
	//need to support distrubuted mode so use Piper instead of chan
	//but for now it support in-memory piper only
	Reader *io.PipeReader
	Writer *io.PipeWriter
}

func NewLocalPipe() *LocalPipe {
	pr, pw := io.Pipe()
	return &LocalPipe{
		Reader: pr,
		Writer: pw,
	}
}
