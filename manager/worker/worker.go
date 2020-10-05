package worker

import (
	"net"

	"github.com/zhnpeng/wstream/manager/worker/pb"
	"google.golang.org/grpc"

	"github.com/coreos/etcd/clientv3"
	sdetcdv3 "github.com/go-kit/kit/sd/etcdv3"
	"gopkg.in/tomb.v1"
)

type Worker interface {
	Key() string
	Endpoint() string
	Run()
	Wait() error
	Kill(reason error)

	// service discovery
	Register() error
	Deregister() error
}

var WorkerKeyPrefix = "/wstream/worker/"

type worker struct {
	client  *clientv3.Client
	address string // ip:port

	registrar sdetcdv3.Registrar
	tomb.Tomb
}

func NewWorker() *worker {
	// TODO: construct registrar
	// reg = sdetcdv3.NewRegistrar()
	// TODO: construct worker object
	return &worker{}
}

func (w *worker) Run() error {
	lis, err := net.Listen("tcp", w.address)
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	pb.RegisterTestServer(srv, NewService())
	if err := srv.Serve(lis); err != nil {
		return err
	}
	return nil
}

// Register service at startup
func (w *worker) Register() {
	w.registrar.Register()
}

func (w *worker) Deregister() {
	w.registrar.Deregister()
}
