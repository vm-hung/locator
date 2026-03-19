package engine

import (
	"context"
	"locator/server/storage"
	"locator/server/transport"
	"log"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type RaftNode struct {
	node        raft.Node
	id          uint64
	peers       []raft.Peer
	ticker      *time.Ticker
	transport   *transport.Transport
	raftStorage *raft.MemoryStorage
	walStorage  *storage.WalStorage

	readStates chan raft.ReadState
	applies    chan ApplyPackage
	stopped    chan struct{}
}

func NewRaftNode(config RaftConfig) *RaftNode {
	memoryStorage, err := config.walStorage.NewMemoryStorage()
	if err != nil {
		log.Fatalf("failed to create memory storage: %v", err)
	}

	raftNode := &RaftNode{
		id:          config.NodeID,
		peers:       config.Peers,
		raftStorage: memoryStorage,
		walStorage:  config.walStorage,
		readStates:  make(chan raft.ReadState, 1),
		applies:     make(chan ApplyPackage),
		stopped:     make(chan struct{}),
	}
	if h := config.heartbeat; h == 0 {
		raftNode.ticker = &time.Ticker{}
	} else {
		raftNode.ticker = time.NewTicker(h)
	}

	raftConfig := &raft.Config{
		ID:              config.NodeID,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         raftNode.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	hs, cs, _ := raftNode.raftStorage.InitialState()
	if raft.IsEmptyHardState(hs) && len(cs.Voters) == 0 {
		log.Printf("Starting new raft node with peers: %+v\n", config.Peers)
		raftNode.node = raft.StartNode(raftConfig, config.Peers)
	} else {
		log.Printf("HardState: %+v\n", hs)
		log.Printf("ConfState: %+v\n", cs)
		log.Printf("Restarting raft node with peers: %+v\n", config.Peers)
		raftNode.node = raft.RestartNode(raftConfig)
	}

	return raftNode
}

func (n *RaftNode) Start(readyHandler RaftReadyHandler) {
	internalTimeout := time.Second

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
				}

				if l := len(rd.ReadStates); l != 0 {
					log.Printf("ReadStates: %+v\n", rd.ReadStates)
					select {
					case n.readStates <- rd.ReadStates[l-1]:
					case <-time.After(internalTimeout):
						log.Printf("timeout send read state")
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
					n.transport.Send(msgs)
				}

				if !raft.IsEmptySnap(rd.Snapshot) {
					log.Printf("Saving snapshot at index %d", rd.Snapshot.Metadata.Index)
					if err := n.walStorage.SaveSnapshot(rd.Snapshot); err != nil {
						log.Printf("failed to save snapshot: %v", err)
					}
				}

				if err := n.walStorage.Save(rd.HardState, rd.Entries); err != nil {
					log.Printf("failed to save hard state and entries: %v", err)
				}

				if !raft.IsEmptySnap(rd.Snapshot) {
					if err := n.walStorage.Flush(); err != nil {
						log.Printf("failed to flush wal storage: %v", err)
					}

					pack.notify <- struct{}{}

					log.Printf("Applying snapshot at index %d", rd.Snapshot.Metadata.Index)
					if err := n.raftStorage.ApplySnapshot(rd.Snapshot); err != nil {
						log.Printf("failed to apply snapshot: %v", err)
					}

					if err := n.walStorage.Release(rd.Snapshot); err != nil {
						log.Printf("failed to release snapshot: %v", err)
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

					n.transport.Send(msgs)
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

func (n *RaftNode) Stop() {
	// TODO: implement
}

func (n *RaftNode) processMessages(msgs []raftpb.Message) []raftpb.Message {
	foundAppResp := false
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Type == raftpb.MsgAppResp {
			if foundAppResp {
				msgs[i].To = 0
			}
			foundAppResp = true
		}
	}
	return msgs
}

func (n *RaftNode) onStop() {
	n.ticker.Stop()
	n.node.Stop()
}

func (n *RaftNode) apply() <-chan ApplyPackage {
	return n.applies
}

func (n *RaftNode) Propose(ctx context.Context, data []byte) error {
	return n.node.Propose(ctx, data)
}

func (n *RaftNode) Process(ctx context.Context, m raftpb.Message) error {
	return n.node.Step(ctx, m)
}
