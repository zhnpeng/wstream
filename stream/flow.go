package stream

import (
	"io"
)

type Flow struct {
}

type XStreamGraph struct {
	id       int
	Vertexes map[int]*StreamNode
}

func NewXStreamGraph() *XStreamGraph {
	return &XStreamGraph{0, make(map[int]*StreamNode)}
}

//OutputSelector 有两种类型
//broadcast	表示广播到所有输出节点
//redistributing 表示重新分布
//比如keyby按照key hash重新分配

type OutputSelector interface {
	Output(inEdges, outEdges []*StreamNode)
}

type BroadcastOutputSelector struct {
}

func (piper *BroadcastOperator) Output(node *StreamNode) {
	for _, ip := range node.InEdges {
		go func(ip *StreamPiper, outEdges []*StreamNode) {
			for {
				var buffer []byte
				_, err := ip.Reader.Read(buffer)
				if err != nil {
					//writepipe is closed or error occur
					for _, on := range outEdges {
						on.Piper.Writer.Close()
					}
				}
				for _, on := range outEdges {
					on.Piper.Writer.Write(buffer)
				}
			}
		}(ip.Piper, node.OutEdges)
	}
}

type StreamPiper struct {
	//need to support distrubuted mode so use Piper instead of chan
	//but for now it support in-memory piper only
	Reader *io.PipeReader
	Writer *io.PipeWriter
}

func NewPiper() *StreamPiper {
	pr, pw := io.Pipe()
	return &StreamPiper{
		Reader: pr,
		Writer: pw,
	}
}

type StreamNode struct {
	ID       int
	Operator Operator
	Piper    *StreamPiper
	InEdges  []*StreamNode
	OutEdges []*StreamNode
	Selector OutputSelector
}

func NewStreamNode(id int) *StreamNode {
	return &StreamNode{
		ID:    id,
		Piper: NewPiper(),
	}
}
