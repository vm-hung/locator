package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"maps"
	"slices"
	"sync"

	"github.com/hunkvm/locator/pkg/types"

	"github.com/cockroachdb/pebble"
)

const (
	serviceKeyPrefix = "service:key"
	serviceRawPrefix = "service:raw"
)

type StateStorage struct {
	db *pebble.DB
}

func NewStateStorage(db *pebble.DB) *StateStorage {
	return &StateStorage{db: db}
}

func (s *StateStorage) Flush() error {
	return s.db.Flush()
}

func (s *StateStorage) Close() error {
	return s.db.Close()
}

func (s *StateStorage) Checkpoint(dir string) error {
	return s.db.Checkpoint(dir)
}

func (s *StateStorage) Persist(srv *types.Service) error {
	sKey := s.buildServiceKey(srv)
	kKey := append([]byte(serviceKeyPrefix+":"), []byte(srv.ID)...)

	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(srv); err != nil {
		return fmt.Errorf("failed to marshal service: %w", err)
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(sKey, buffer.Bytes(), pebble.NoSync); err != nil {
		return fmt.Errorf("failed to persist service: %w", err)
	}
	if err := batch.Set(kKey, []byte(sKey), pebble.NoSync); err != nil {
		return fmt.Errorf("failed to persist service: %w", err)
	}
	return batch.Commit(pebble.Sync)
}

func (s *StateStorage) Remove(id string) (*types.Service, error) {
	var service *types.Service
	batch := s.db.NewBatch()
	defer batch.Close()
	kKey := append([]byte(serviceKeyPrefix+":"), []byte(id)...)
	if sKey, closer, err := batch.Get(kKey); err == nil {
		defer closer.Close()
		if data, closer, err := batch.Get(sKey); err == nil {
			defer closer.Close()
			buffer := bytes.NewReader(data)
			decoder := gob.NewDecoder(buffer)
			if err := decoder.Decode(&service); err != nil {
				return nil, fmt.Errorf("failed to unmarshal service: %w", err)
			}
		}

		if err := batch.Delete(sKey, pebble.NoSync); err != nil {
			return nil, fmt.Errorf("failed to remove service: %w", err)
		}
	}
	if err := batch.Delete(kKey, pebble.NoSync); err != nil {
		return nil, fmt.Errorf("failed to remove service: %w", err)
	}
	return service, batch.Commit(pebble.Sync)
}

func (s *StateStorage) Retrieve(selector map[string][]any) ([]*types.Service, error) {
	components := []string{
		"Metadata.Node", "Metadata.Region", "Metadata.Zone",
		"Address.Host", "Address.Port",
	}
	return s.retrieveRecursive(serviceRawPrefix, components, selector)
}

func (s *StateStorage) buildServiceKey(srv *types.Service) []byte {
	node := srv.Metadata["Node"]
	if node == "" {
		node = types.DefaultNode
	}
	region := srv.Metadata["Region"]
	if region == "" {
		region = types.DefaultRegion
	}
	zone := srv.Metadata["Zone"]
	if zone == "" {
		zone = types.DefaultZone
	}

	return fmt.Appendf(
		nil, serviceRawPrefix+":%s:%s:%s:%s:%d", node, region,
		zone, srv.Address.Host, srv.Address.Port,
	)
}

func (s *StateStorage) retrieveRecursive(
	prefix string, components []string, selector map[string][]any,
) ([]*types.Service, error) {
	if len(components) == 0 {
		return s.searchWithPrefix(prefix+":", selector)
	}
	component := components[0]
	values, ok := selector[component]
	newSelector := maps.Clone(selector)
	delete(newSelector, component)

	if !ok || len(values) == 0 {
		return s.searchWithPrefix(prefix+":", newSelector)
	}

	if len(components) == 1 && component == "Address.Port" {
		keys := make([][]byte, 0, len(values))
		for _, value := range values {
			k := fmt.Sprintf("%s:%v", prefix, value)
			keys = append(keys, []byte(k))
		}
		return s.retrieveService(keys)
	}

	var (
		allServices []*types.Service
		waitGroup   sync.WaitGroup
		once        sync.Once
		mutex       sync.Mutex
		firstError  error
	)

	for _, value := range values {
		waitGroup.Add(1)
		go func(v any) {
			defer waitGroup.Done()
			newPrefix := prefix + ":" + fmt.Sprintf("%v", v)
			services, err := s.retrieveRecursive(newPrefix, components[1:], newSelector)
			if err != nil {
				once.Do(func() { firstError = err })
				return
			}
			if len(services) > 0 {
				mutex.Lock()
				allServices = append(allServices, services...)
				mutex.Unlock()
			}
		}(value)
	}
	waitGroup.Wait()

	return allServices, firstError
}

func (s *StateStorage) searchWithPrefix(
	prefix string, selector map[string][]any,
) ([]*types.Service, error) {
	var services []*types.Service
	prefixKey := []byte(prefix)

	iter, _ := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefixKey,
		UpperBound: upperbound(prefixKey),
	})

	defer func() {
		if err := iter.Close(); err != nil {
			log.Printf("iterator error: %v", err)
		}
	}()

	for iter.First(); iter.Valid(); iter.Next() {
		var srv types.Service
		buffer := bytes.NewReader(iter.Value())
		decoder := gob.NewDecoder(buffer)
		if err := decoder.Decode(&srv); err != nil {
			return nil, fmt.Errorf("failed to unmarshal service: %w", err)
		}
		if s.serviceMatches(&srv, selector) {
			services = append(services, &srv)
		}
	}
	return services, nil
}

func (s *StateStorage) retrieveService(keys [][]byte) ([]*types.Service, error) {
	var (
		services   []*types.Service
		waitGroup  sync.WaitGroup
		once       sync.Once
		mutex      sync.Mutex
		firstError error
	)

	for _, key := range keys {
		waitGroup.Add(1)
		go func(k []byte) {
			defer waitGroup.Done()
			data, closer, err := s.db.Get(k)
			if err != nil {
				once.Do(func() { firstError = err })
				return
			}
			defer closer.Close()
			var srv types.Service
			buffer := bytes.NewReader(data)
			decoder := gob.NewDecoder(buffer)
			if err := decoder.Decode(&srv); err != nil {
				once.Do(func() { firstError = err })
				return
			}
			mutex.Lock()
			defer mutex.Unlock()
			services = append(services, &srv)
		}(key)
	}
	waitGroup.Wait()

	return services, firstError
}

func (s *StateStorage) serviceMatches(
	srv *types.Service, selector map[string][]any,
) bool {
	for key, values := range selector {
		if len(values) == 0 {
			continue
		}
		var actual any
		switch key {
		case "ID":
			actual = srv.ID
		case "Name":
			actual = srv.Name
		case "Address.Host":
			actual = srv.Address.Host
		case "Address.Port":
			actual = srv.Address.Port
		case "HealthStatus":
			actual = srv.HealthStatus
		case "Enabled":
			actual = srv.Enabled
		case "Metadata.Node":
			actual = srv.Metadata["Node"]
			if actual == "" {
				actual = types.DefaultNode
			}
		case "Metadata.Region":
			actual = srv.Metadata["Region"]
			if actual == "" {
				actual = types.DefaultRegion
			}
		case "Metadata.Zone":
			actual = srv.Metadata["Zone"]
			if actual == "" {
				actual = types.DefaultZone
			}
		default:
			continue
		}

		if !slices.Contains(values, actual) {
			return false
		}
	}
	return true
}
