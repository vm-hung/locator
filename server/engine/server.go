package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"locator/model"
	"locator/pkg/idgen"
	"locator/pkg/notify"
	"locator/pkg/schedule"
	"locator/pkg/wait"
	"locator/server/storage"
	"locator/server/transport"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type LocatorServer struct {
	raftNode *RaftNode
	leader   atomic.Uint64
	storage  *storage.StateStorage

	appliedIndex   atomic.Uint64
	committedIndex atomic.Uint64

	idGenerator   *idgen.Generator
	callback      LocatorServerCallback
	readNotifier  *notify.Completion
	leaderChanged *notify.Broadcaster
	applyWait     wait.Deadline
	readSignal    chan struct{}
	stopped       chan struct{}
}

func NewLocatorServer(config Config) (*LocatorServer, error) {
	raftDir := filepath.Join(config.DataDir, "raft")
	stateDir := filepath.Join(config.DataDir, "state")

	stateDB, err := storage.NewPebbleDB(stateDir)
	if err != nil {
		format := "failed to create state storage: %w"
		return nil, fmt.Errorf(format, err)
	}
	stateStorage := storage.NewStateStorage(stateDB)

	raftDB, err := storage.NewPebbleDB(raftDir)
	if err != nil {
		format := "failed to create raft storage: %w"
		return nil, fmt.Errorf(format, err)
	}
	walStorage := storage.NewWalStorage(raftDB)

	addrs := strings.Split(config.PeerAddrs, ",")
	transport := transport.NewTransport()

	var peers []raft.Peer
	for _, addr := range addrs {
		parts := strings.Split(addr, "@")
		id, err := strconv.ParseUint(parts[0], 10, 64)
		if len(parts) != 2 || err != nil {
			return nil, fmt.Errorf("invalid peer address: %s", addr)
		}
		peers = append(peers, raft.Peer{ID: id})
		transport.AddPeer(id, parts[1])
	}

	raftConfig := RaftConfig{
		NodeID:     config.NodeID,
		Peers:      peers,
		walStorage: walStorage,
		heartbeat:  100 * time.Millisecond,
	}
	raftNode := NewRaftNode(raftConfig)
	raftNode.transport = transport

	return &LocatorServer{
		raftNode:      raftNode,
		storage:       stateStorage,
		idGenerator:   idgen.New(config.NodeID),
		readNotifier:  notify.NewCompletion(),
		leaderChanged: notify.NewBroadcaster(),
		applyWait:     wait.NewDeadline(),
		readSignal:    make(chan struct{}, 1),
		stopped:       make(chan struct{}),
	}, nil
}

func (s *LocatorServer) Start(callback LocatorServerCallback) {
	s.callback = callback
	go s.linearizableReadLoop()
	go s.run()
}

func (s *LocatorServer) run() {
	snapshot, err := s.raftNode.raftStorage.Snapshot()
	if err != nil {
		panic("failed to get snapshot")
	}

	queue := schedule.NewJobQueue()

	readyHandler := RaftReadyHandler{
		currentLeader: func() uint64 {
			return s.leader.Load()
		},
		updateLeader: func(leader uint64) {
			s.leader.Store(leader)
		},
		newLeadership: func(newLeader bool) {
			if newLeader {
				s.leaderChanged.Notify()
				if s.leader.Load() == s.raftNode.id {
					s.callback.OnLeaderChanged(true)
				} else {
					s.callback.OnLeaderChanged(false)
				}
			}
		},
		setCommittedIdx: func(idx uint64) {
			if idx > s.committedIndex.Load() {
				s.committedIndex.Store(idx)
			}
		},
	}

	s.raftNode.Start(readyHandler)

	progress := LocatorProgress{
		configState:   snapshot.Metadata.ConfState,
		diskSnapIndex: snapshot.Metadata.Index,
		memSnapIndex:  snapshot.Metadata.Index,
		appliedTerm:   snapshot.Metadata.Term,
		appliedIndex:  snapshot.Metadata.Index,
	}

	defer func() {
		// TODO: implement
		queue.Stop()
		s.raftNode.Stop()
	}()

	for {
		select {
		case pack := <-s.raftNode.apply():
			job := schedule.NewJob(
				"LocatorServer.applyPackage",
				func(ctx context.Context) {
					s.applyPackage(&progress, &pack)
				},
			)
			queue.Schedule(job)
		case <-s.stopped:
			return
		}
	}
}

func (s *LocatorServer) linearizableReadLoop() {
	for {
		leaderChanged := s.leaderChanged.Receive()
		select {
		case <-s.readSignal:
		case <-leaderChanged:
			continue
		case <-s.stopped:
			return
		}

		notifier := s.readNotifier
		nextNotifier := notify.NewCompletion()
		s.readNotifier = nextNotifier

		confirmedIndex, err := s.requestCurrentIndex()
		if err != nil {
			notifier.Notify(err)
			continue
		}

		appliedIndex := s.appliedIndex.Load()
		if appliedIndex < confirmedIndex {
			select {
			case <-s.applyWait.Wait(confirmedIndex):
			case <-s.stopped:
				return
			}
		}
		notifier.Notify(nil)
	}
}

func (s *LocatorServer) requestCurrentIndex() (uint64, error) {
	requestIDs := map[uint64]struct{}{}
	requestID := s.idGenerator.Next()
	requestIDs[requestID] = struct{}{}
	err := s.sendReadIndex(requestID)
	if err != nil {
		return 0, err
	}

	requestTimeout := 5*time.Second + 2*time.Duration(100*10)*time.Microsecond
	// 5s + 2 * election timeout
	timeoutTimer := time.NewTimer(requestTimeout)
	defer timeoutTimer.Stop()
	retryTimer := time.NewTimer(500 * time.Millisecond)
	defer retryTimer.Stop()

	for {
		select {
		case rs := <-s.raftNode.readStates:
			responseID := uint64(0)
			if len(rs.RequestCtx) == 8 {
				bigEndian := binary.BigEndian
				responseID = bigEndian.Uint64(rs.RequestCtx)
			}
			if _, ok := requestIDs[responseID]; !ok {
				continue
			}
			return rs.Index, nil
		case <-retryTimer.C:
			log.Printf("Retrying read index: %d", requestID)
			requestID = s.idGenerator.Next()
			requestIDs[requestID] = struct{}{}
			err := s.sendReadIndex(requestID)
			if err != nil {
				return 0, err
			}
			retryTimer.Reset(500 * time.Millisecond)
			continue
		case <-timeoutTimer.C:
			log.Printf("timed out for read index")
			return 0, errors.New("timeout")
		case <-s.stopped:
			return 0, errors.New("stopped")
		}
	}
}

func (s *LocatorServer) sendReadIndex(requestID uint64) error {
	parent, timeout := context.Background(), time.Second
	ctx, cancel := context.WithTimeout(parent, timeout)

	defer cancel()
	bytes := make([]byte, 8)

	binary.BigEndian.PutUint64(bytes, requestID)
	err := s.raftNode.node.ReadIndex(ctx, bytes)
	if err != nil {
		log.Printf("failed to read index: %v", err)
		return err
	}
	return nil
}

func (s *LocatorServer) applyPackage(pg *LocatorProgress, pack *ApplyPackage) {
	s.applySnapshot(pg, pack)
	s.applyEntries(pg, pack)

	s.applyWait.Trigger(pg.appliedIndex)

	<-pack.notify
	s.snapshotIfNeeded()
}

func (s *LocatorServer) applySnapshot(_ *LocatorProgress, pack *ApplyPackage) {
	if raft.IsEmptySnap(pack.snapshot) {
		return
	}
	<-pack.notify
	log.Printf("applySnapshot: %+v\n", pack.snapshot)
}

func (s *LocatorServer) applyEntries(pg *LocatorProgress, pack *ApplyPackage) {
	if len(pack.entries) == 0 {
		return
	}

	firstIndex := pack.entries[0].Index
	if firstIndex > pg.appliedIndex+1 {
		format := "unexpected committed entry index: %d, %d"
		log.Fatalf(format, firstIndex, pg.appliedIndex)
	}

	var shouldApplyEntries []raftpb.Entry
	if pg.appliedIndex+1-firstIndex < uint64(len(pack.entries)) {
		shouldApplyEntries = pack.entries[pg.appliedIndex+1-firstIndex:]
	}
	if len(shouldApplyEntries) == 0 {
		return
	}

	var appliedTerm, appliedIndex uint64
	for _, entry := range shouldApplyEntries {
		switch entry.Type {
		case raftpb.EntryNormal:
			if len(entry.Data) > 0 {
				s.applyEntryNormal(entry)
			}
			s.appliedIndex.Store(entry.Index)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(entry.Data)
			s.raftNode.node.ApplyConfChange(cc)
			s.appliedIndex.Store(entry.Index)
			log.Printf("EntryConfChange: %+v\n", entry)
		default:
			log.Printf("Unknown entry type: %v\n", entry.Type)
		}
		appliedTerm = entry.Term
		appliedIndex = entry.Index
	}

	pg.appliedIndex = appliedIndex
	pg.appliedTerm = appliedTerm
}

func (s *LocatorServer) applyEntryNormal(entry raftpb.Entry) {
	buffer := bytes.NewBuffer(entry.Data)
	commandType, _ := buffer.ReadByte()

	switch commandType {
	case CMD_CREATE:
		var srv model.Service
		decoder := gob.NewDecoder(buffer)
		if err := decoder.Decode(&srv); err != nil {
			log.Printf("failed to decode service: %v", err)
			return
		}
		if err := s.storage.Persist(&srv); err != nil {
			log.Printf("failed to persist service: %v", err)
			return
		}
		go s.callback.OnServicePersisted(&srv)
		return
	case CMD_REMOVE:
		id := buffer.String()
		if srv, err := s.storage.Remove(id); err != nil {
			log.Printf("failed to remove service: %v", err)
			return
		} else {
			go s.callback.OnServiceRemoved(srv)
		}
		return
	case CMD_UPDATE:
		var srv model.Service
		decoder := gob.NewDecoder(buffer)
		if err := decoder.Decode(&srv); err != nil {
			log.Printf("failed to decode service: %v", err)
			return
		}
		if err := s.storage.Persist(&srv); err != nil {
			log.Printf("failed to persist service: %v", err)
			return
		}
		go s.callback.OnServiceUpdated(&srv)
		return
	default:
		log.Printf("Unknown command type: %v", commandType)
		return
	}
}

func (s *LocatorServer) snapshotIfNeeded() {

}

func (s *LocatorServer) waitForReadSafety(ctx context.Context) error {
	notifier := s.readNotifier

	select {
	case s.readSignal <- struct{}{}:
		log.Println("Read signal sent")
	default:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-notifier.Signal():
		return notifier.Error()
	}
}

func (s *LocatorServer) Process(ctx context.Context, m raftpb.Message) error {
	return s.raftNode.Process(ctx, m)
}

func (s *LocatorServer) Retrieve(
	ctx context.Context, selector map[string][]any,
) ([]*model.Service, error) {
	if err := s.waitForReadSafety(ctx); err != nil {
		return nil, err
	}
	return s.storage.Retrieve(selector)
}

func (s *LocatorServer) Create(ctx context.Context, srv *model.Service) error {
	buffer := bytes.NewBuffer([]byte{byte(CMD_CREATE)})
	encoder := gob.NewEncoder(buffer)
	if err := encoder.Encode(*srv); err != nil {
		log.Printf("failed to encode service: %v", err)
		return err
	}
	log.Printf("Proposing persist service: %+v\n", srv)
	return s.raftNode.Propose(ctx, buffer.Bytes())
}

func (s *LocatorServer) Update(ctx context.Context, srv *model.Service) error {
	buffer := bytes.NewBuffer([]byte{byte(CMD_UPDATE)})
	encoder := gob.NewEncoder(buffer)
	if err := encoder.Encode(*srv); err != nil {
		log.Printf("failed to encode service: %v", err)
		return err
	}
	log.Printf("Proposing update service: %+v", srv)
	return s.raftNode.Propose(ctx, buffer.Bytes())
}

func (s *LocatorServer) Remove(ctx context.Context, id string) error {
	buffer := bytes.NewBuffer([]byte{byte(CMD_REMOVE)})
	binary.Write(buffer, binary.BigEndian, id)
	log.Printf("Proposing deregister service: %s\n", id)
	return s.raftNode.Propose(ctx, buffer.Bytes())
}
