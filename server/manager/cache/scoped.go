package cache

import (
	"context"
	"fmt"
	"sync"

	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"go.uber.org/zap"
)

type watchEntry struct {
	id     uint64
	cancel func()
}

type scopedState struct {
	mutex       sync.RWMutex
	logger      *zap.SugaredLogger
	caches      map[string]*cache.LinearCache
	watches     map[uint64]watchEntry
	nextWatchID uint64
}

func (s *scopedState) getOrCreateCache(typ string) *cache.LinearCache {
	lc, ok := s.caches[typ]
	if !ok {
		lc = cache.NewLinearCache(
			typ,
			cache.WithLogger(s.logger),
		)
		s.caches[typ] = lc
	}
	return lc
}

func (s *scopedState) getCache(typ string) *cache.LinearCache {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if lc, ok := s.caches[typ]; ok {
		return lc
	}
	return nil
}

func (s *scopedState) getResource(typ, name string) types.Resource {
	if lc := s.getCache(typ); lc != nil {
		return lc.GetResources()[name]
	}
	return nil
}

func (s *scopedState) upsertResource(typ, name string, res types.Resource) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	cache := s.getOrCreateCache(typ)
	return cache.UpdateResource(name, res)
}

func (s *scopedState) updateResource(typ, name string, res types.Resource) error {
	if lc := s.getCache(typ); lc != nil {
		return lc.UpdateResource(name, res)
	}
	return fmt.Errorf("resource %s not found", name)
}

func (s *scopedState) deleteResource(typ, name string) error {
	if lc := s.getCache(typ); lc != nil {
		return lc.DeleteResource(name)
	}
	return fmt.Errorf("resource %s not found", name)
}

func (s *scopedState) applyResources(
	resources map[string]map[string]types.Resource,
) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for typ, res := range resources {
		lc := s.getOrCreateCache(typ)
		lc.SetResources(res)
	}
}

func (s *scopedState) clearResources() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, entry := range s.watches {
		entry.cancel()
	}
	s.nextWatchID = 0
	s.watches = make(map[uint64]watchEntry)
}

func (s *scopedState) cleanupIfEmpty(typ string) {
	if lc, ok := s.caches[typ]; ok {
		nr := lc.NumResources()
		nw := lc.NumCacheWatches()
		if nr == 0 && nw == 0 {
			delete(s.caches, typ)
		}
	}
}

func (s *scopedState) isEmptyState() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	numResources, numWatches := 0, 0
	for _, lc := range s.caches {
		numResources += lc.NumResources()
		numWatches += lc.NumCacheWatches()
	}
	return numResources == 0 && numWatches == 0
}

func (s *scopedState) addWatch(typ string, cancel func()) func() {
	watchID := s.nextWatchID
	s.nextWatchID++

	watch := watchEntry{
		id: watchID,
		cancel: func() {
			cancel()
			s.cleanupIfEmpty(typ)
		},
	}
	s.watches[watchID] = watch

	var once sync.Once
	return func() {
		once.Do(func() {
			s.mutex.Lock()
			defer s.mutex.Unlock()
			if entry, ok := s.watches[watchID]; ok {
				entry.cancel()
				delete(s.watches, watchID)
			}
		})
	}
}

func (s *scopedState) createWatch(
	req *cache.Request, sub cache.Subscription,
	out chan cache.Response,
) (func(), error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	lc := s.getOrCreateCache(req.TypeUrl)
	cancel, err := lc.CreateWatch(req, sub, out)
	if err != nil {
		return nil, err
	}
	return s.addWatch(req.TypeUrl, cancel), nil
}

func (s *scopedState) createDeltaWatch(
	req *cache.DeltaRequest, sub cache.Subscription,
	out chan cache.DeltaResponse,
) (func(), error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	lc := s.getOrCreateCache(req.TypeUrl)
	cancel, err := lc.CreateDeltaWatch(req, sub, out)
	if err != nil {
		return nil, err
	}
	return s.addWatch(req.TypeUrl, cancel), nil
}

type ScopedCache struct {
	mutex  sync.RWMutex
	logger *zap.Logger
	hash   cache.NodeHash
	states map[string]*scopedState
}

var _ cache.Cache = &ScopedCache{}

func NewScopedCache(hash cache.NodeHash, logger *zap.Logger) *ScopedCache {
	return &ScopedCache{
		hash: hash, logger: logger,
		states: make(map[string]*scopedState),
	}
}

func (c *ScopedCache) getOrCreateState(key string) *scopedState {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	state, ok := c.states[key]
	if !ok {
		state = &scopedState{
			logger:  c.logger.Sugar(),
			caches:  make(map[string]*cache.LinearCache),
			watches: make(map[uint64]watchEntry),
		}
		c.states[key] = state
	}
	return state
}

func (c *ScopedCache) getState(key string) *scopedState {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if st, ok := c.states[key]; ok {
		return st
	}
	return nil
}

func (c *ScopedCache) cleanupIfEmpty(key string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if st, ok := c.states[key]; ok {
		if st.isEmptyState() {
			delete(c.states, key)
		}
	}
}

func (c *ScopedCache) CreateWatch(
	req *cache.Request, sub cache.Subscription,
	out chan cache.Response,
) (func(), error) {
	key := c.hash.ID(req.Node)
	state := c.getOrCreateState(key)

	cancel, err := state.createWatch(req, sub, out)
	if err != nil {
		return nil, err
	}

	return func() {
		cancel()
		c.cleanupIfEmpty(key)
	}, nil
}

func (c *ScopedCache) CreateDeltaWatch(
	req *cache.DeltaRequest, sub cache.Subscription,
	out chan cache.DeltaResponse,
) (func(), error) {
	key := c.hash.ID(req.Node)
	state := c.getOrCreateState(key)

	cancel, err := state.createDeltaWatch(req, sub, out)
	if err != nil {
		return nil, err
	}

	return func() {
		cancel()
		c.cleanupIfEmpty(key)
	}, nil
}

func (c *ScopedCache) Fetch(
	ctx context.Context, req *cache.Request,
) (cache.Response, error) {
	key, typ := c.hash.ID(req.Node), req.TypeUrl
	if st := c.getState(key); st != nil {
		if lc := st.getCache(typ); lc != nil {
			return lc.Fetch(ctx, req)
		}
	}
	return nil, nil
}

func (c *ScopedCache) ApplyResources(
	key string,
	resources map[string]map[string]types.Resource,
) {
	state := c.getOrCreateState(key)
	state.applyResources(resources)
}

func (c *ScopedCache) ClearResources(key string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if st, ok := c.states[key]; ok {
		st.clearResources()
		delete(c.states, key)
	}
}

func (c *ScopedCache) GetResource(key, typ, name string) types.Resource {
	if st := c.getState(key); st != nil {
		return st.getResource(typ, name)
	}
	return nil
}

func (c *ScopedCache) UpsertResource(key, typ, name string, res types.Resource) error {
	state := c.getOrCreateState(key)
	return state.upsertResource(typ, name, res)
}

func (c *ScopedCache) UpdateResource(key, typ, name string, res types.Resource) error {
	if st := c.getState(key); st != nil {
		return st.updateResource(typ, name, res)
	}
	return fmt.Errorf("resource %s on key %s not found", name, key)
}

func (c *ScopedCache) DeleteResource(key, typ, name string) error {
	if st := c.getState(key); st != nil {
		return st.deleteResource(typ, name)
	}
	return fmt.Errorf("resource %s on key %s not found", name, key)
}
