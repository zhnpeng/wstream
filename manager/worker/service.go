package worker

import (
	"context"
	"fmt"

	grpctransport "github.com/go-kit/kit/transport/grpc"

	"github.com/go-kit/kit/endpoint"
	"github.com/zhnpeng/wstream/manager/worker/pb"
)

type Service interface {
	Test(ctx context.Context, a string, b int64) (context.Context, string, error)
}

type TestRequest struct {
	A string
	B int64
}

type TestResponse struct {
	Ctx context.Context
	V   string
}

type service struct{}

func (service) Test(ctx context.Context, a string, b int64) (context.Context, string, error) {
	return nil, fmt.Sprintf("%s = %d", a, b), nil
}

func NewService() Service {
	return service{}
}

func makeTestEndpoint(svc Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(TestRequest)
		newCtx, v, err := svc.Test(ctx, req.A, req.B)
		return &TestResponse{
			V:   v,
			Ctx: newCtx,
		}, err
	}
}

type serverBinding struct {
	test grpctransport.Handler
}

func (b *serverBinding) Test(ctx context.Context, req *pb.TestRequest) (*pb.TestResponse, error) {
	_, response, err := b.test.ServeGRPC(ctx, req)
	if err != nil {
		return nil, err
	}
	return response.(*pb.TestResponse), nil
}

func NewBinding(svc Service) *serverBinding {
	return &serverBinding{
		test: grpctransport.NewServer(
			makeTestEndpoint(svc),
			decodeRequest,
			encodeResponse,
			grpctransport.ServerBefore(
				extractCorrelationID,
			),
			grpctransport.ServerBefore(
				displayServerRequestHeaders,
			),
			grpctransport.ServerAfter(
				injectResponseHeader,
				injectResponseTrailer,
				injectConsumedCorrelationID,
			),
			grpctransport.ServerAfter(
				displayServerResponseHeaders,
				displayServerResponseTrailers,
			),
		),
	}
}
