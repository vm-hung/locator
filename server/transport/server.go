package transport

import (
	"context"
	"encoding/binary"
	"io"
	"log"

	"github.com/hunkvm/locator/pkg/proto/transport"

	"go.etcd.io/raft/v3/raftpb"
)

type MessageProcessor interface {
	Process(context.Context, raftpb.Message) error
	Recover(context.Context, *SnapshotMessage) error
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
	log.Printf("Handle snapshot stream")
	pipeReader, pipeWriter := io.Pipe()
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)
		for {
			req, err := stream.Recv()
			if err == io.EOF {
				pipeWriter.Close()
				return
			}
			if err != nil {
				pipeWriter.CloseWithError(err)
				return
			}
			data := req.GetChunk()
			if _, err := pipeWriter.Write(data); err != nil {
				pipeWriter.CloseWithError(err)
				return
			}
		}
	}()

	header := make([]byte, 8)
	if _, err := io.ReadFull(pipeReader, header); err != nil {
		return err
	}

	msgSize := int(binary.BigEndian.Uint64(header))
	msgData := make([]byte, msgSize)
	if _, err := io.ReadFull(pipeReader, msgData); err != nil {
		return err
	}

	var message raftpb.Message
	if err := message.Unmarshal(msgData); err != nil {
		return err
	}

	msg := NewSnapshotMessage(message, pipeReader)
	if err := s.Processor.Recover(stream.Context(), msg); err != nil {
		return err
	}

	select {
	case ok := <-msg.CloseNotify():
		res := &transport.StreamDataResponse{Success: ok}
		return stream.SendAndClose(res)
	case <-stopped:
		return io.ErrUnexpectedEOF
	}
}
