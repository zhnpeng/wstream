package wio

import "io"

//LocalPipe is pipe in memory
type LocalPipe struct {
	//need to support distrubuted mode so use Piper instead of chan
	//but for now it support in-memory piper only
	Reader     *io.PipeReader
	Writer     *io.PipeWriter
	SourceNode string
	TargetNode string
}

func NewLocalPipe() *LocalPipe {
	pr, pw := io.Pipe()
	return &LocalPipe{
		Reader: pr,
		Writer: pw,
	}
}

func (p *LocalPipe) Read() (data []byte, err error) {
	_, err = p.Reader.Read(data)
	if err != nil {
		return data, err
	}
	return data, nil
}

func (p *LocalPipe) Write(data []byte) (n int, err error) {
	return p.Writer.Write(data)
}
