package engine

import (
	"locator/model"
	"locator/server/storage"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	CMD_CREATE = 0x01
	CMD_REMOVE = 0x02
	CMD_UPDATE = 0x03
)

type RaftConfig struct {
	NodeID     uint64
	Peers      []raft.Peer
	heartbeat  time.Duration
	walStorage *storage.WalStorage
}

type ApplyPackage struct {
	snapshot raftpb.Snapshot
	entries  []raftpb.Entry
	notify   chan struct{}
}

type LocatorServerCallback struct {
	OnLeaderChanged    func(bool)
	OnServicePersisted func(*model.Service)
	OnServiceUpdated   func(*model.Service)
	OnServiceRemoved   func(*model.Service)
}

type RaftReadyHandler struct {
	currentLeader   func() uint64
	updateLeader    func(uint64)
	newLeadership   func(bool)
	setCommittedIdx func(uint64)
}

type LocatorProgress struct {
	configState   raftpb.ConfState
	diskSnapIndex uint64
	memSnapIndex  uint64
	appliedTerm   uint64
	appliedIndex  uint64
}
