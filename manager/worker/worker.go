package worker

import (
	"github.com/coreos/etcd/clientv3"
	"gopkg.in/tomb.v1"
)

type Worker interface {
	Register() error
	Deregister() error
	Bind() error
	Service() (Service, error)
	Run()
	Wait() error
	Kill(reason error)
}

var WorkerKeyPrefix = "/wstream/worker/"

type worker struct {
	client   *clientv3.Client
	endpoint string

	tomb.Tomb
}

func NewWorker() *worker {
	// TODO: construct worker object
	return &worker{}
}

func (w *worker) Run() {

}

func (w *worker) Register() error {

}

func (w *worker) Service() (Service, error) {
	return NewService(), nil
}
