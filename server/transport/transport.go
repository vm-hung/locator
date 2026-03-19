package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/hunkvm/locator/pkg/proto/transport"
	"go.uber.org/zap"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Transport struct {
	mutex  sync.RWMutex
	logger *zap.Logger
	peers  map[uint64]transport.RaftTransportServiceClient
}

func NewTransport(logger *zap.Logger) *Transport {
	return &Transport{
		logger: logger,
		peers:  make(map[uint64]transport.RaftTransportServiceClient),
	}
}

func (s *Transport) Stop() {
	// TODO: implement
}

func (s *Transport) AddPeer(id uint64, addr string) {
	credentials := insecure.NewCredentials()
	option := grpc.WithTransportCredentials(credentials)
	conn, err := grpc.NewClient(addr, option)
	if err != nil {
		s.logger.Error("failed to create client", zap.Error(err))
		return
	}

	client := transport.NewRaftTransportServiceClient(conn)
	s.mutex.Lock()
	s.peers[id] = client
	s.mutex.Unlock()
}

func (s *Transport) SendMessage(messages []raftpb.Message) {
	for _, message := range messages {
		if message.To == 0 {
			continue
		}
		to := message.To

		s.mutex.RLock()
		client, ok := s.peers[to]
		s.mutex.RUnlock()

		if !ok {
			s.logger.Warn("no client for peer", zap.Uint64("peer_id", to))
			continue
		}

		data, err := message.Marshal()
		if err != nil {
			s.logger.Error("failed to marshal message", zap.Error(err))
			continue
		}

		go func(m raftpb.Message, d []byte) {
			req := &transport.SendMessageRequest{Data: d}
			_, err := client.Send(context.Background(), req)
			if err != nil {
				s.logger.Error(
					"failed to send message to peer",
					zap.Uint64("peer_id", m.To), zap.Error(err),
				)
			}
		}(message, data)
	}
}

func (s *Transport) SendSnapshot(msg SnapshotMessage) {
	s.mutex.RLock()
	client, ok := s.peers[msg.Message.To]
	s.mutex.RUnlock()

	if !ok {
		s.logger.Warn(
			"no client for sending snapshot",
			zap.Uint64("peer_id", msg.Message.To),
		)
		msg.CloseWithError(errors.New("no client for peer"))
		return
	}

	go func(m SnapshotMessage) {
		stream, err := client.Stream(context.Background())
		if err != nil {
			m.CloseWithError(err)
			return
		}

		msgData, err := m.Message.Marshal()
		if err != nil {
			m.CloseWithError(err)
			return
		}

		header, msgSize := make([]byte, 8), len(msgData)
		binary.BigEndian.PutUint64(header, uint64(msgSize))
		hBuffer, mBuffer := bytes.NewBuffer(header), bytes.NewBuffer(msgData)
		reader := io.MultiReader(hBuffer, mBuffer, m.ReadCloser)

		buffer := make([]byte, 64*1024)
		for {
			if n, err := reader.Read(buffer); n > 0 {
				req := &transport.StreamDataRequest{Chunk: buffer[:n]}
				if err := stream.Send(req); err != nil {
					m.CloseWithError(err)
					return
				}
			} else {
				if err == io.EOF {
					break
				}
				if err != nil {
					m.CloseWithError(err)
					return
				}
			}
		}

		res, err := stream.CloseAndRecv()
		if err != nil {
			m.CloseWithError(err)
			return
		}
		s.logger.Info(
			"send snapshot success",
			zap.Any("response", res),
		)
		m.CloseWithError(nil)
	}(msg)
}

func (t *Transport) ActiveSince(id uint64) time.Time {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	// TODO: implement
	return time.Time{}
}
