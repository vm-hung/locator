package transport

import (
	"context"

	"locator/api/transport"

	"go.etcd.io/raft/v3/raftpb"
)

type MessageProcessor interface {
	Process(ctx context.Context, m raftpb.Message) error
}

type TransportServer struct {
	Processor MessageProcessor
	transport.UnimplementedRaftTransportServiceServer
}

func NewTransportServer(processor MessageProcessor) *TransportServer {
	return &TransportServer{Processor: processor}
}

func (s *TransportServer) Send(
	ctx context.Context, req *transport.SendMessageRequest,
) (*transport.SendMessageResponse, error) {
	var message raftpb.Message
	if err := message.Unmarshal(req.Data); err != nil {
		return &transport.SendMessageResponse{Success: false}, err
	}
	if err := s.Processor.Process(ctx, message); err != nil {
		return &transport.SendMessageResponse{Success: false}, err
	}
	return &transport.SendMessageResponse{Success: true}, nil
}

func (s *TransportServer) Stream(
	stream transport.RaftTransportService_StreamServer,
) error {
	return nil
}
