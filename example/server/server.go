package server

import (
	"context"
	"fmt"
	hello "github.com/hunkvm/locator/example/api"
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
