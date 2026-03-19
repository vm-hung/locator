package engine

import (
	"context"
	"time"

	"github.com/hunkvm/locator/server/storage"
	"github.com/hunkvm/locator/server/transport"
	"go.uber.org/zap"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	maxInFlightMsgSnap = 16
	maxSizePerMsg      = 1 * 1024 * 1024
	maxInflightMsgs    = 4096 / 8
	internalTimeout    = 1 * time.Second
)

type raftConfig struct {
	id           uint64
	logger       *zap.Logger
	peers        []raft.Peer
	walStorage   *storage.WalStorage
	heartbeat    time.Duration
	electionTick int
}

type raftNode struct {
	id          uint64
	logger      *zap.Logger
	node        raft.Node
	peers       []raft.Peer
	ticker      *time.Ticker
	transport   *transport.Transport
	raftStorage *raft.MemoryStorage
	walStorage  *storage.WalStorage

	electionTick int
	heartbeat    time.Duration

	readStates  chan raft.ReadState
	msgSnapshot chan raftpb.Message
	applies     chan ApplyPackage
	stopped     chan struct{}
	done        chan struct{}
}

func newRaftNode(config raftConfig) *raftNode {
	raft.SetLogger(newRaftLogger(config.logger))
	memoryStorage, err := config.walStorage.NewMemoryStorage()
	if err != nil {
		config.logger.Fatal("failed to create memory storage", zap.Error(err))
	}

	mif := maxInFlightMsgSnap
	raftNode := &raftNode{
		id:           config.id,
		logger:       config.logger,
		peers:        config.peers,
		raftStorage:  memoryStorage,
		walStorage:   config.walStorage,
		electionTick: config.electionTick,
		heartbeat:    config.heartbeat,
		readStates:   make(chan raft.ReadState, 1),
		msgSnapshot:  make(chan raftpb.Message, mif),
		applies:      make(chan ApplyPackage),
		stopped:      make(chan struct{}),
		done:         make(chan struct{}),
	}
	if h := config.heartbeat; h == 0 {
		raftNode.ticker = &time.Ticker{}
	} else {
		raftNode.ticker = time.NewTicker(h)
	}

	raftConfig := &raft.Config{
		ID:              config.id,
		ElectionTick:    config.electionTick,
		HeartbeatTick:   1,
		Storage:         raftNode.raftStorage,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
	}

	hs, cs, _ := raftNode.raftStorage.InitialState()
	if raft.IsEmptyHardState(hs) && len(cs.Voters) == 0 {
		raftNode.logger.Info(
			"starting new raft node",
			zap.Any("peers", config.peers),
		)
		raftNode.node = raft.StartNode(raftConfig, config.peers)
	} else {
		raftNode.logger.Info(
			"restarting raft node",
			zap.Any("peers", config.peers),
		)
		raftNode.node = raft.RestartNode(raftConfig)
	}

	return raftNode
}

func (n *raftNode) Start(readyHandler ReadyHandler) {

	go func() {
		defer n.onStop()
		isLeader := false

		for {
			select {
			case <-n.ticker.C:
				n.node.Tick()
			case rd := <-n.node.Ready():
				if rd.SoftState != nil {
					currentLeader := readyHandler.currentLeader()
					newLeader := rd.SoftState.Lead != raft.None &&
						rd.SoftState.Lead != currentLeader
					isLeader = rd.RaftState == raft.StateLeader
					readyHandler.updateLeader(rd.SoftState.Lead)
					readyHandler.newLeadership(newLeader)
					if newLeader {
						n.logger.Info(
							"raft leadership changed",
							zap.Uint64("leader", rd.SoftState.Lead),
						)
					}
				}

				if l := len(rd.ReadStates); l != 0 {
					select {
					case n.readStates <- rd.ReadStates[l-1]:
					case <-time.After(internalTimeout):
						n.logger.Warn("timeout send read state")
					case <-n.stopped:
						return
					}
				}

				pack := ApplyPackage{
					entries:  rd.CommittedEntries,
					snapshot: rd.Snapshot,
					notify:   make(chan struct{}, 1),
				}

				var ci uint64
				if len(pack.entries) != 0 {
					ci = pack.entries[len(pack.entries)-1].Index
				}
				if pack.snapshot.Metadata.Index > ci {
					ci = pack.snapshot.Metadata.Index
				}
				if ci != 0 {
					readyHandler.setCommittedIdx(ci)
				}

				select {
				case n.applies <- pack:
				case <-n.stopped:
					return
				}

				if isLeader {
					msgs := n.processMessages(rd.Messages)
					n.transport.SendMessage(msgs)
				}

				if !raft.IsEmptySnap(rd.Snapshot) {
					n.logger.Info(
						"saving raft snapshot from leader",
						zap.Uint64("index", rd.Snapshot.Metadata.Index),
					)
					if err := n.walStorage.SaveSnapshot(rd.Snapshot); err != nil {
						n.logger.Error("failed to save snapshot", zap.Error(err))
					}
				}

				if err := n.walStorage.Save(rd.HardState, rd.Entries); err != nil {
					n.logger.Error("failed to save hard state and entries", zap.Error(err))
				}

				if !raft.IsEmptySnap(rd.Snapshot) {
					if err := n.walStorage.Flush(); err != nil {
						n.logger.Error("failed to flush wal storage", zap.Error(err))
					}

					pack.notify <- struct{}{}

					n.logger.Info("applying snapshot", zap.Uint64("index", rd.Snapshot.Metadata.Index))
					if err := n.raftStorage.ApplySnapshot(rd.Snapshot); err != nil {
						n.logger.Error("failed to apply snapshot", zap.Error(err))
					}

					if err := n.walStorage.TruncateEntries(rd.Snapshot); err != nil {
						n.logger.Error("failed to release snapshot", zap.Error(err))
					}
				}

				n.raftStorage.Append(rd.Entries)

				configChanged := false
				for _, ent := range rd.CommittedEntries {
					if ent.Type == raftpb.EntryConfChange {
						configChanged = true
						break
					}
				}

				if !isLeader {
					msgs := n.processMessages(rd.Messages)
					pack.notify <- struct{}{}

					if configChanged {
						select {
						case pack.notify <- struct{}{}:
						case <-n.stopped:
							return
						}
					}

					n.transport.SendMessage(msgs)
				} else {
					pack.notify <- struct{}{}
				}

				n.node.Advance()
			case <-n.stopped:
				return
			}
		}
	}()
}

func (n *raftNode) processMessages(msgs []raftpb.Message) []raftpb.Message {
	foundAppResp := false
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Type == raftpb.MsgAppResp {
			if foundAppResp {
				msgs[i].To = 0
			}
			foundAppResp = true
		}

		if msgs[i].Type == raftpb.MsgSnap {
			select {
			case n.msgSnapshot <- msgs[i]:
			default:
				n.logger.Warn(
					"snapshot channel full, dropping message",
					zap.Uint64("to", msgs[i].To),
				)
			}
			msgs[i].To = 0
		}
	}
	return msgs
}

func (n *raftNode) stop() {
	select {
	case n.stopped <- struct{}{}:
	case <-n.done:
		return
	}
	<-n.done
}

func (n *raftNode) onStop() {
	n.node.Stop()
	n.ticker.Stop()
	n.transport.Stop()
	if err := n.walStorage.Close(); err != nil {
		n.logger.Error("failed to close wal", zap.Error(err))
	}
	close(n.done)
}

func (n *raftNode) apply() <-chan ApplyPackage {
	return n.applies
}

func (n *raftNode) Propose(ctx context.Context, data []byte) error {
	return n.node.Propose(ctx, data)
}

func (n *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return n.node.Step(ctx, m)
}
