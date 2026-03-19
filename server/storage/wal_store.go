package storage

import (
	"encoding/binary"
	"log"

	"github.com/cockroachdb/pebble"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var (
	keyHardState = []byte("hardstate")
	keyConfState = []byte("confstate")
	keySnapshot  = []byte("snapshot")
	prefixEntry  = []byte("entry:")
)

type WalStorage struct {
	db        *pebble.DB
	hardState raftpb.HardState
}

func NewWalStorage(db *pebble.DB) *WalStorage {
	return &WalStorage{db: db}
}

func (s *WalStorage) NewMemoryStorage() (*raft.MemoryStorage, error) {
	memory := raft.NewMemoryStorage()
	if val, closer, err := s.db.Get(keyHardState); err == nil {
		var hs raftpb.HardState
		if err := hs.Unmarshal(val); err == nil {
			memory.SetHardState(hs)
		}
		closer.Close()
	}

	if val, closer, err := s.db.Get(keySnapshot); err == nil {
		var snap raftpb.Snapshot
		if err := snap.Unmarshal(val); err == nil {
			memory.ApplySnapshot(snap)
		}
		closer.Close()
	}

	iter, _ := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefixEntry,
		UpperBound: upperbound(prefixEntry),
	})
	var entries []raftpb.Entry
	for iter.First(); iter.Valid(); iter.Next() {
		var ent raftpb.Entry
		if err := ent.Unmarshal(iter.Value()); err != nil {
			iter.Close()
			return nil, err
		}
		entries = append(entries, ent)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	if len(entries) > 0 {
		memory.Append(entries)
	}

	return memory, nil
}

func (s *WalStorage) Flush() error {
	return s.db.Flush()
}

func (s *WalStorage) Close() error {
	return s.db.Close()
}

func (s *WalStorage) SaveSnapshot(snap raftpb.Snapshot) error {
	if data, err := snap.Marshal(); err == nil {
		return s.db.Set(keySnapshot, data, pebble.Sync)
	} else {
		return err
	}
}

func (s *WalStorage) Save(hs raftpb.HardState, entries []raftpb.Entry) error {
	if raft.IsEmptyHardState(hs) && len(entries) == 0 {
		return nil
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	for i := range entries {
		data, err := entries[i].Marshal()
		if err != nil {
			return err
		}
		key := s.encodeIndex(entries[i].Index)
		if err := batch.Set(key, data, pebble.NoSync); err != nil {
			return err
		}
	}

	if !raft.IsEmptyHardState(hs) {
		if data, err := hs.Marshal(); err != nil {
			return err
		} else {
			if err := batch.Set(keyHardState, data, pebble.NoSync); err != nil {
				return err
			}
		}
	}

	opts := pebble.NoSync
	if raft.MustSync(hs, s.hardState, len(entries)) {
		opts = pebble.Sync
	}
	return batch.Commit(opts)
}

func (s *WalStorage) Release(snap raftpb.Snapshot) error {
	log.Printf("Releasing snapshot at index %d", snap.Metadata.Index)
	return nil
}

func (s *WalStorage) encodeIndex(i uint64) []byte {
	bytes := make([]byte, 14)
	copy(bytes, prefixEntry)
	binary.BigEndian.PutUint64(bytes[6:], i)
	return bytes
}

func (s *WalStorage) decodeIndex(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[6:])
}
