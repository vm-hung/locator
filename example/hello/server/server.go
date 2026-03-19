package server

import (
	"context"
	"fmt"
	hello "locator/example/hello/api"
)

type HelloServer struct {
	hello.UnimplementedHelloServiceServer
}

func NewServer() *HelloServer {
	return &HelloServer{}
}

func (s *HelloServer) SayHello(
	ctx context.Context, req *hello.SayHelloRequest,
) (*hello.SayHelloResponse, error) {
	msg := fmt.Sprintf("Hello, %s!", req.GetName())
	return &hello.SayHelloResponse{Message: msg}, nil
}
