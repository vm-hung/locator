package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hunkvm/locator/pkg/idgen"
	"github.com/hunkvm/locator/pkg/notify"
	"github.com/hunkvm/locator/pkg/schedule"
	"github.com/hunkvm/locator/pkg/types"
	"github.com/hunkvm/locator/pkg/wait"
	"github.com/hunkvm/locator/server/config"
	"github.com/hunkvm/locator/server/storage"
	"github.com/hunkvm/locator/server/transport"
	"go.uber.org/zap"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	delayAfterSnapshot = 30 * time.Second
	// TODO: change this value, it is too small
	memorySnapshotCount   = uint64(5)
	defaultSnapshotCount  = uint64(15)
	defaultCatchUpEntries = uint64(10)

	defaultElectionTick = 10
	defaultHeartbeat    = 100 * time.Millisecond

	raftLogDir    = "raft-log"
	stateStoreDir = "state-store"
)

type EngineConfig struct {
	Logger  *zap.Logger
	Node    uint64
	DataDir string
	Raft    config.RaftConfig
}

type LocatorEngine struct {
	logger   *zap.Logger
	raftNode *raftNode
	leader   atomic.Uint64
	storage  *storage.StateStorage

	context context.Context
	cancel  context.CancelFunc

	snapshotCount  uint64
	catchUpEntries uint64

	readMutex sync.RWMutex
	waitMutex sync.RWMutex
	waitGroup sync.WaitGroup

	dataDirectory     string
	inflightSnapshots atomic.Int64
	appliedIndex      atomic.Uint64
	committedIndex    atomic.Uint64

	idGenerator   *idgen.Generator
	callback      EngineCallback
	readNotifier  *notify.Completion
	leaderChanged *notify.Broadcaster
	applyWait     wait.Deadline
	readSignal    chan struct{}
	stopped       chan struct{}
	stopping      chan struct{}

	// TODO: check if this is needed
	done chan struct{}
}

func NewLocatorEngine(cfg EngineConfig) (*LocatorEngine, error) {
	raftDir := filepath.Join(cfg.DataDir, raftLogDir)
	stateDir := filepath.Join(cfg.DataDir, stateStoreDir)

	sLogger := cfg.Logger.Named("state")
	stateDB, err := storage.NewPebbleDB(stateDir, sLogger)
	if err != nil {
		format := "failed to create state storage: %w"
		return nil, fmt.Errorf(format, err)
	}
	stateStorage := storage.NewStateStorage(stateDB)

	rLogger := cfg.Logger.Named("raft")
	raftDB, err := storage.NewPebbleDB(raftDir, rLogger)
	if err != nil {
		format := "failed to create raft storage: %w"
		return nil, fmt.Errorf(format, err)
	}
	walStorage := storage.NewWalStorage(raftDB)

	addrs := strings.Split(cfg.Raft.PeerAddrs, ",")
	tLogger := cfg.Logger.Named("transport")
	transport := transport.NewTransport(tLogger)

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

	zLogger := cfg.Logger.Named("raft")

	raftConfig := raftConfig{
		logger:     zLogger,
		id:         cfg.Node,
		peers:      peers,
		walStorage: walStorage,
	}

	if tick := cfg.Raft.ElectionTick; tick == 0 {
		raftConfig.electionTick = defaultElectionTick
	} else {
		raftConfig.electionTick = tick
	}

	if duration := cfg.Raft.Heartbeat; duration == 0 {
		raftConfig.heartbeat = defaultHeartbeat
	} else {
		raftConfig.heartbeat = duration
	}

	raftNode := newRaftNode(raftConfig)
	raftNode.transport = transport

	locatorEngine := &LocatorEngine{
		logger:        cfg.Logger,
		raftNode:      raftNode,
		storage:       stateStorage,
		dataDirectory: cfg.DataDir,
		idGenerator:   idgen.New(cfg.Node),
	}

	if count := cfg.Raft.SnapshotCount; count == 0 {
		locatorEngine.snapshotCount = defaultSnapshotCount
	} else {
		locatorEngine.snapshotCount = count
	}

	if count := cfg.Raft.CatchupEntries; count == 0 {
		locatorEngine.catchUpEntries = defaultCatchUpEntries
	} else {
		locatorEngine.catchUpEntries = count
	}
	return locatorEngine, nil
}

func (e *LocatorEngine) GoAttach(fn func()) {
	e.waitMutex.RLock()
	defer e.waitMutex.RUnlock()

	select {
	case <-e.stopping:
		e.logger.Warn("server is stopping")
	default:
	}

	e.waitGroup.Go(fn)
}

func (e *LocatorEngine) Start(callback EngineCallback) {
	e.start(callback)
	e.GoAttach(e.linearizableReadLoop)
}

func (e *LocatorEngine) Stop() {
	if err := e.tryTransferLeadership(); err != nil {
		e.logger.Error("failed to transfer leadership", zap.Error(err))
	}
	// TODO: not close done channel here
	close(e.done)
	e.forceStop()
}

func (e *LocatorEngine) start(callback EngineCallback) {
	ctx, cancel := context.WithCancel(context.Background())
	e.callback, e.context, e.cancel = callback, ctx, cancel

	e.applyWait = wait.NewDeadline()

	e.readSignal = make(chan struct{}, 1)
	e.stopped = make(chan struct{})
	e.done = make(chan struct{})
	e.stopping = make(chan struct{}, 1)

	e.readNotifier = notify.NewCompletion()
	e.leaderChanged = notify.NewBroadcaster()

	go e.run()
}

func (e *LocatorEngine) run() {
	snapshot, err := e.raftNode.raftStorage.Snapshot()
	if err != nil {
		panic("failed to get snapshot")
	}

	queue := schedule.NewJobQueue()

	readyHandler := ReadyHandler{
		currentLeader: func() uint64 {
			return e.leader.Load()
		},
		updateLeader: func(leader uint64) {
			e.leader.Store(leader)
		},
		newLeadership: func(newLeader bool) {
			if newLeader {
				e.leaderChanged.Notify()
				if e.leader.Load() == e.raftNode.id {
					go e.callback.OnLeaderChanged(true)
				} else {
					go e.callback.OnLeaderChanged(false)
				}
			}
		},
		setCommittedIdx: func(idx uint64) {
			if idx > e.committedIndex.Load() {
				e.committedIndex.Store(idx)
			}
		},
	}

	e.raftNode.Start(readyHandler)

	progress := LocatorProgress{
		configState:     snapshot.Metadata.ConfState,
		diskSnapIndex:   snapshot.Metadata.Index,
		memorySnapIndex: snapshot.Metadata.Index,
		appliedTerm:     snapshot.Metadata.Term,
		appliedIndex:    snapshot.Metadata.Index,
	}

	defer func() {
		// TODO: implement, not go here
		e.logger.Info("stopping locator server")
		queue.Stop()
		e.raftNode.stop()
		close(e.done)
	}()

	for {
		select {
		case pack := <-e.raftNode.apply():
			job := schedule.NewJob(
				"LocatorServer.applyPackage",
				func(ctx context.Context) {
					e.applyPackage(&progress, &pack)
				},
			)
			queue.Schedule(job)
		case <-e.stopped:
			return
		}
	}
}

func (e *LocatorEngine) linearizableReadLoop() {
	for {
		leaderChanged := e.leaderChanged.Receive()
		select {
		case <-e.readSignal:
		case <-leaderChanged:
			continue
		case <-e.stopped:
			return
		}

		notifier := e.readNotifier
		e.readMutex.Lock()
		nextNotifier := notify.NewCompletion()
		e.readNotifier = nextNotifier
		e.readMutex.Unlock()

		confirmedIndex, err := e.requestCurrentIndex()
		if err != nil {
			notifier.Notify(err)
			continue
		}

		appliedIndex := e.appliedIndex.Load()
		if appliedIndex < confirmedIndex {
			select {
			case <-e.applyWait.Wait(confirmedIndex):
			case <-e.stopped:
				return
			}
		}
		notifier.Notify(nil)
	}
}

func (e *LocatorEngine) requestCurrentIndex() (uint64, error) {
	requestIDs := map[uint64]struct{}{}
	requestID := e.idGenerator.Next()
	requestIDs[requestID] = struct{}{}
	err := e.sendReadIndex(requestID)
	if err != nil {
		return 0, err
	}

	timeoutTimer := time.NewTimer(e.requestTimeout())
	defer timeoutTimer.Stop()
	retryTimer := time.NewTimer(500 * time.Millisecond)
	defer retryTimer.Stop()

	for {
		select {
		case rs := <-e.raftNode.readStates:
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
			e.logger.Warn(
				"retrying read index",
				zap.Uint64("request_id", requestID),
			)
			requestID = e.idGenerator.Next()
			requestIDs[requestID] = struct{}{}
			err := e.sendReadIndex(requestID)
			if err != nil {
				return 0, err
			}
			retryTimer.Reset(500 * time.Millisecond)
			continue
		case <-timeoutTimer.C:
			e.logger.Warn("timed out for read index")
			return 0, errors.New("timeout")
		case <-e.stopped:
			return 0, errors.New("stopped")
		}
	}
}

func (s *LocatorEngine) requestTimeout() time.Duration {
	e, h := s.raftNode.electionTick, s.raftNode.heartbeat
	electionInterval := time.Duration(e * int(h))
	return 5*time.Second + 2*electionInterval*time.Microsecond
}

func (e *LocatorEngine) sendReadIndex(requestID uint64) error {
	parent, timeout := context.Background(), time.Second
	ctx, cancel := context.WithTimeout(parent, timeout)

	defer cancel()
	bytes := make([]byte, 8)

	binary.BigEndian.PutUint64(bytes, requestID)
	err := e.raftNode.node.ReadIndex(ctx, bytes)
	if err != nil {
		e.logger.Error("failed to read index", zap.Error(err))
		return err
	}
	return nil
}

func (e *LocatorEngine) applyPackage(lg *LocatorProgress, pack *ApplyPackage) {
	e.applySnapshot(lg, pack)
	e.applyEntries(lg, pack)

	e.applyWait.Trigger(lg.appliedIndex)

	<-pack.notify
	e.snapshotAndCompactRaftLogIfNeed(lg)

	select {
	case msg := <-e.raftNode.msgSnapshot:
		m := e.createSnapshotMessage(msg, lg)
		e.sendSnapshotMessage(m)
	default:
		// if no snapshot message, just ignore
	}
}

func (e *LocatorEngine) applySnapshot(lg *LocatorProgress, pack *ApplyPackage) {
	if raft.IsEmptySnap(pack.snapshot) {
		return
	}

	if pack.snapshot.Metadata.Index <= lg.appliedIndex {
		e.logger.Fatal("unexpected snapshot index",
			zap.Uint64("snap_index", pack.snapshot.Metadata.Index),
			zap.Uint64("applied_index", lg.appliedIndex),
		)
	}

	<-pack.notify

	snapIndex := pack.snapshot.Metadata.Index
	name := fmt.Sprintf("snapshot-%d", snapIndex)
	dir := filepath.Join(e.dataDirectory, name)

	if err := e.storage.Close(); err != nil {
		e.logger.Fatal("failed to close storage", zap.Error(err))
	}

	stateDir := filepath.Join(e.dataDirectory, stateStoreDir)
	if err := os.RemoveAll(stateDir); err != nil {
		e.logger.Fatal("failed to remove old state store", zap.Error(err))
	}

	if err := os.Rename(dir, stateDir); err != nil {
		e.logger.Fatal("failed to rename temp dir", zap.Error(err))
	}

	sLogger := e.logger.Named("state")
	db, err := storage.NewPebbleDB(stateDir, sLogger)
	if err != nil {
		e.logger.Fatal("failed to open new state store", zap.Error(err))
	}
	e.storage = storage.NewStateStorage(db)

	lg.diskSnapIndex = pack.snapshot.Metadata.Index
	lg.memorySnapIndex = pack.snapshot.Metadata.Index
	lg.appliedTerm = pack.snapshot.Metadata.Term
	lg.appliedIndex = pack.snapshot.Metadata.Index
	lg.configState = pack.snapshot.Metadata.ConfState
	e.logger.Info("applied snapshot", zap.Uint64("index", lg.appliedIndex))
	if e.callback.OnSnapshotApplied != nil {
		go e.callback.OnSnapshotApplied(lg.appliedIndex)
	}
}

func (e *LocatorEngine) applyEntries(lg *LocatorProgress, pack *ApplyPackage) {
	if len(pack.entries) == 0 {
		return
	}

	firstIndex := pack.entries[0].Index
	if firstIndex > lg.appliedIndex+1 {
		e.logger.Fatal("unexpected committed entry index",
			zap.Uint64("first_index", firstIndex),
			zap.Uint64("applied_index", lg.appliedIndex),
		)
	}

	var shouldApplyEntries []raftpb.Entry
	if lg.appliedIndex+1-firstIndex < uint64(len(pack.entries)) {
		shouldApplyEntries = pack.entries[lg.appliedIndex+1-firstIndex:]
	}
	if len(shouldApplyEntries) == 0 {
		return
	}

	var appliedTerm, appliedIndex uint64
	for _, entry := range shouldApplyEntries {
		switch entry.Type {
		case raftpb.EntryNormal:
			if len(entry.Data) > 0 {
				e.applyEntryNormal(entry)
			}
			e.appliedIndex.Store(entry.Index)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(entry.Data); err != nil {
				e.logger.Error("failed to unmarshal conf change", zap.Error(err))
				continue
			}
			e.logger.Info("conf change entry", zap.Any("entry", entry))
			lg.configState = *e.raftNode.node.ApplyConfChange(cc)
			e.appliedIndex.Store(entry.Index)
		default:
			e.logger.Warn("unknown entry type", zap.Any("type", entry.Type))
		}
		appliedTerm = entry.Term
		appliedIndex = entry.Index
	}

	lg.appliedIndex = appliedIndex
	lg.appliedTerm = appliedTerm
}

func (e *LocatorEngine) applyEntryNormal(entry raftpb.Entry) {
	buffer := bytes.NewBuffer(entry.Data)
	commandType, _ := buffer.ReadByte()

	switch commandType {
	case CMD_CREATE:
		var srv types.Service
		decoder := gob.NewDecoder(buffer)
		if err := decoder.Decode(&srv); err != nil {
			e.logger.Error("failed to decode service", zap.Error(err))
			return
		}
		if err := e.storage.Persist(&srv); err != nil {
			e.logger.Error("failed to persist service", zap.Error(err))
			return
		}
		if e.callback.OnServicePersisted != nil {
			go e.callback.OnServicePersisted(srv)
		}
		return
	case CMD_REMOVE:
		id := buffer.String()
		if srv, err := e.storage.Remove(id); err != nil {
			e.logger.Error("failed to remove service", zap.Error(err))
			return
		} else if srv != nil {
			if e.callback.OnServiceRemoved != nil {
				go e.callback.OnServiceRemoved(*srv)
			}
		}
		return
	case CMD_UPDATE:
		var srv types.Service
		decoder := gob.NewDecoder(buffer)
		if err := decoder.Decode(&srv); err != nil {
			e.logger.Error("failed to decode service", zap.Error(err))
			return
		}
		if err := e.storage.Persist(&srv); err != nil {
			e.logger.Error("failed to persist service", zap.Error(err))
			return
		}
		if e.callback.OnServiceUpdated != nil {
			go e.callback.OnServiceUpdated(srv)
		}
		return
	default:
		e.logger.Warn("unknown command type", zap.Uint8("type", commandType))
		return
	}
}

func (e *LocatorEngine) snapshotAndCompactRaftLogIfNeed(lg *LocatorProgress) {
	shouldSnapshotToDisk := e.shouldSnapshotToDisk(lg)
	shouldSnapshotToMemory := e.shouldSnapshotToMemory(lg)
	if !shouldSnapshotToDisk && !shouldSnapshotToMemory {
		return
	}
	e.takeSnapshot(lg, shouldSnapshotToDisk)
	e.compactRaftLog(lg.appliedIndex)
}

func (e *LocatorEngine) shouldSnapshotToDisk(lg *LocatorProgress) bool {
	return lg.appliedIndex-lg.diskSnapIndex > e.snapshotCount
}

func (e *LocatorEngine) shouldSnapshotToMemory(lg *LocatorProgress) bool {
	return lg.appliedIndex > lg.memorySnapIndex+memorySnapshotCount
}

func (e *LocatorEngine) takeSnapshot(lg *LocatorProgress, toDisk bool) {
	if toDisk {
		if err := e.raftNode.walStorage.Flush(); err != nil {
			e.logger.Error("failed to flush storage", zap.Error(err))
			return
		}
	}
	idx, conf := lg.appliedIndex, lg.configState

	snap, err := e.raftNode.raftStorage.CreateSnapshot(idx, &conf, nil)
	e.logger.Info("taking memory snapshot", zap.Uint64("index", idx))
	if err != nil {
		if errors.Is(err, raft.ErrSnapOutOfDate) {
			return
		}
		e.logger.Fatal("failed to create snapshot", zap.Error(err))
	}
	lg.memorySnapIndex = lg.appliedIndex

	if toDisk {
		if err = e.raftNode.walStorage.SaveSnapshot(snap); err != nil {
			e.logger.Fatal("failed to save snapshot", zap.Error(err))
		}
		lg.diskSnapIndex = lg.appliedIndex
		if err = e.raftNode.walStorage.TruncateEntries(snap); err != nil {
			e.logger.Fatal("failed to truncate entries", zap.Error(err))
		}
		e.logger.Info("taking disk snapshot", zap.Uint64("index", lg.appliedIndex))
	}
}

func (e *LocatorEngine) compactRaftLog(snapshotIndex uint64) {
	if e.inflightSnapshots.Load() != 0 {
		e.logger.Info("inflight snapshots, skipping compact")
		return
	}

	compactIndex := uint64(1)
	// TODO: make it configurable
	snapCatchUpEntries := defaultCatchUpEntries
	if c := snapCatchUpEntries; snapshotIndex > c {
		compactIndex = snapshotIndex - c
	}

	err := e.raftNode.raftStorage.Compact(compactIndex)
	if err != nil {
		if errors.Is(err, raft.ErrCompacted) {
			return
		}
		e.logger.Fatal("failed to compact raft log", zap.Error(err))
	}
	e.logger.Info("compacted raft log", zap.Uint64("index", compactIndex))
}

func (e *LocatorEngine) waitForReadSafety(ctx context.Context) error {
	e.readMutex.RLock()
	notifier := e.readNotifier
	e.readMutex.RUnlock()

	select {
	case e.readSignal <- struct{}{}:
		e.logger.Info("read signal sent")
	default:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-notifier.Signal():
		return notifier.Error()
	}
}

func (e *LocatorEngine) forceStop() {
	select {
	case e.stopped <- struct{}{}:
	case <-e.done:
		return
	}
	<-e.done
}

func (e *LocatorEngine) tryTransferLeadership() error {
	if !e.isLeader() {
		e.logger.Info("not leader, skipping transfer leadership")
		return nil
	}

	if e.singleNode() {
		e.logger.Info("single node, skipping transfer leadership")
		return nil
	}

	// TODO: implement

	// transport, peers := s.raftNode.transport, s.raftNode.peers
	// nextLeader, ok := longestConnected(transport, peers)
	// if !ok {
	// 	e.logger.Warn("no active peers, skipping transfer leadership")
	// 	// TODO: centralize this error
	// 	return errors.New("has no active peers")
	// }

	// ctx, cancel := context.WithTimeout(s.context, s.requestTimeout())
	// err := s.moveLeadership(ctx, s.leader.Load(), nextLeader)
	// cancel()
	// return err

	return nil
}

func (e *LocatorEngine) isLeader() bool {
	return e.raftNode.id == e.leader.Load()
}

func (e *LocatorEngine) singleNode() bool {
	return len(e.raftNode.peers) == 1
}

func (e *LocatorEngine) moveLeadership(context.Context, uint64, uint64) error {
	// TODO: implement
	return nil
}

func (e *LocatorEngine) Process(ctx context.Context, m raftpb.Message) error {
	return e.raftNode.Process(ctx, m)
}

func (e *LocatorEngine) Recover(ctx context.Context, m *transport.SnapshotMessage) error {
	e.saveSnapshotMessage(*m)
	return e.raftNode.Process(ctx, m.Message)
}

func (e *LocatorEngine) Retrieve(
	ctx context.Context, selector map[string][]any,
) ([]*types.Service, error) {
	if err := e.waitForReadSafety(ctx); err != nil {
		return nil, err
	}
	return e.storage.Retrieve(selector)
}

func (e *LocatorEngine) Create(ctx context.Context, srv types.Service) error {
	buffer := bytes.NewBuffer([]byte{byte(CMD_CREATE)})
	encoder := gob.NewEncoder(buffer)
	if err := encoder.Encode(srv); err != nil {
		e.logger.Error("failed to encode service", zap.Error(err))
		return err
	}
	e.logger.Info("proposing persist service", zap.Any("service", srv))
	return e.raftNode.Propose(ctx, buffer.Bytes())
}

func (e *LocatorEngine) Update(ctx context.Context, srv types.Service) error {
	buffer := bytes.NewBuffer([]byte{byte(CMD_UPDATE)})
	encoder := gob.NewEncoder(buffer)
	if err := encoder.Encode(srv); err != nil {
		e.logger.Error("failed to encode service", zap.Error(err))
		return err
	}
	e.logger.Info("proposing update service", zap.Any("service", srv))
	return e.raftNode.Propose(ctx, buffer.Bytes())
}

func (e *LocatorEngine) Remove(ctx context.Context, id string) error {
	buffer := bytes.NewBuffer([]byte{byte(CMD_REMOVE)})
	binary.Write(buffer, binary.BigEndian, id)
	e.logger.Info("proposing deregister service", zap.String("id", id))
	return e.raftNode.Propose(ctx, buffer.Bytes())
}
