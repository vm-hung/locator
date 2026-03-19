package engine

import (
	"time"

	"github.com/hunkvm/locator/server/transport"

	"go.etcd.io/raft/v3"
)

func longestConnected(trans *transport.Transport, peers []raft.Peer) (uint64, bool) {
	var longest uint64
	var oldest time.Time
	for _, p := range peers {
		time := trans.ActiveSince(p.ID)
		if time.IsZero() {
			continue
		}

		if oldest.IsZero() {
			oldest = time
			longest = p.ID
		}

		if time.Before(oldest) {
			oldest = time
			longest = p.ID
		}
	}
	if longest == 0 {
		return longest, false
	}
	return longest, true
}
