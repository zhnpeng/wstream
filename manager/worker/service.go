package worker

import (
	"context"
	"fmt"

	"github.com/zhnpeng/wstream/manager/worker/pb"
)

type service struct {
	pb.TestServer
}

func (service) Test(ctx context.Context, req *pb.TestRequest) (*pb.TestResponse, error) {
	return &pb.TestResponse{
		V: fmt.Sprintf("%s = %d", req.A, req.B),
	}, nil
}

func NewService() pb.TestServer {
	return service{}
}
