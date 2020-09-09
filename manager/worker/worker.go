package worker

import (
	"github.com/coreos/etcd/clientv3"
	sdetcdv3 "github.com/go-kit/kit/sd/etcdv3"
	"gopkg.in/tomb.v1"
)

type Worker interface {
	Key() string
	Endpoint() string
	Bind() error
	Service() (Service, error)
	Run()
	Wait() error
	Kill(reason error)

	// service discovery
	Register() error
	Deregister() error
}

var WorkerKeyPrefix = "/wstream/worker/"

type worker struct {
	client   *clientv3.Client
	endpoint string

	registrar sdetcdv3.Registrar
	tomb.Tomb
}

func NewWorker() *worker {
	// TODO: construct registrar
	// reg = sdetcdv3.NewRegistrar()
	// TODO: construct worker object
	return &worker{}
}

func (w *worker) Run() {
}

// Register service at startup
func (w *worker) Register() {
	w.registrar.Register()
}

func (w *worker) Deregister() {
	w.registrar.Deregister()
}

func (w *worker) Service() (Service, error) {
	return NewService(), nil
}
