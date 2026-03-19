package transport

import (
	"context"
	"locator/api/transport"
	"log"
	"sync"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Transport struct {
	mutex sync.RWMutex
	peers map[uint64]transport.RaftTransportServiceClient
}

func NewTransport() *Transport {
	return &Transport{
		peers: make(map[uint64]transport.RaftTransportServiceClient),
	}
}

func (s *Transport) AddPeer(id uint64, addr string) {
	credentials := insecure.NewCredentials()
	option := grpc.WithTransportCredentials(credentials)
	conn, err := grpc.NewClient(addr, option)
	if err != nil {
		log.Printf("failed to create client: %v", err)
		return
	}

	client := transport.NewRaftTransportServiceClient(conn)
	s.mutex.Lock()
	s.peers[id] = client
	s.mutex.Unlock()
}

func (s *Transport) Send(messages []raftpb.Message) error {
	for _, message := range messages {
		if message.To == 0 {
			continue
		}
		to := message.To

		s.mutex.RLock()
		client, ok := s.peers[to]
		s.mutex.RUnlock()

		if !ok {
			log.Printf("no client for peer %d", to)
			continue
		}

		data, err := message.Marshal()
		if err != nil {
			log.Printf("failed to marshal message: %v", err)
			continue
		}

		go func(m raftpb.Message, data []byte) {
			req := &transport.SendMessageRequest{Data: data}
			_, err := client.Send(context.Background(), req)
			if err != nil {
				format := "failed to send message to peer %d: %v"
				log.Printf(format, m.To, err)
			}
		}(message, data)
	}
	return nil
}
