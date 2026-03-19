package transport

import (
	"io"

	"go.etcd.io/raft/v3/raftpb"
)

type SnapshotMessage struct {
	Message    raftpb.Message
	ReadCloser io.ReadCloser
	close      chan bool
}

func NewSnapshotMessage(rs raftpb.Message, rc io.ReadCloser) *SnapshotMessage {
	return &SnapshotMessage{
		Message: rs, ReadCloser: rc,
		close: make(chan bool, 1),
	}
}

func (m SnapshotMessage) CloseNotify() <-chan bool {
	return m.close
}

func (m SnapshotMessage) CloseWithError(err error) {
	if cerr := m.ReadCloser.Close(); cerr != nil {
		err = cerr
	}
	if err == nil {
		m.close <- true
	} else {
		m.close <- false
	}
}
