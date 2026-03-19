package engine

import (
	"github.com/hunkvm/locator/pkg/types"

	"go.etcd.io/raft/v3/raftpb"
)

const (
	CMD_CREATE = 0x01
	CMD_REMOVE = 0x02
	CMD_UPDATE = 0x03
)

type ApplyPackage struct {
	snapshot raftpb.Snapshot
	entries  []raftpb.Entry
	notify   chan struct{}
}

type EngineCallback struct {
	OnLeaderChanged    func(bool)
	OnServicePersisted func(types.Service)
	OnServiceUpdated   func(types.Service)
	OnServiceRemoved   func(types.Service)
	OnSnapshotApplied  func(uint64)
}

type ReadyHandler struct {
	currentLeader   func() uint64
	updateLeader    func(uint64)
	newLeadership   func(bool)
	setCommittedIdx func(uint64)
}

type LocatorProgress struct {
	configState     raftpb.ConfState
	memorySnapIndex uint64
	diskSnapIndex   uint64
	appliedTerm     uint64
	appliedIndex    uint64
}
