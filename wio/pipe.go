package wio

type Pipe interface {
	Read() (data []byte, err error)
	Write(data []byte) (n int, err error)
}
