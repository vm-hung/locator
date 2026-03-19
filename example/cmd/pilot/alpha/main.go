package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	hello "github.com/hunkvm/locator/example/api"

	healthsrv "google.golang.org/grpc/health"
	health "google.golang.org/grpc/health/grpc_health_v1"

	"google.golang.org/grpc"
)

var logger = log.New(os.Stdout, "[alpha] ", log.LstdFlags)

type alphaServer struct {
	hello.UnimplementedHelloServiceServer
}

func (s *alphaServer) SayHello(
	ctx context.Context, req *hello.SayHelloRequest,
) (*hello.SayHelloResponse, error) {
	logger.Printf("SayHello name=%q", req.GetName())
	return &hello.SayHelloResponse{
		Message: fmt.Sprintf("[alpha] Hello, %s!", req.GetName()),
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50061")
	if err != nil {
		logger.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	healthServer := healthsrv.NewServer()

	health.RegisterHealthServer(grpcServer, healthServer)
	hello.RegisterHelloServiceServer(grpcServer, &alphaServer{})
	healthServer.SetServingStatus("hello", health.HealthCheckResponse_SERVING)

	logger.Println("listening on :50061")
	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatalf("failed to serve: %v", err)
	}
}
