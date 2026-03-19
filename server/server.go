package server

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/hunkvm/locator/server/config"
	"github.com/hunkvm/locator/server/engine"
	"github.com/hunkvm/locator/server/manager"
	registry "github.com/hunkvm/locator/server/service"
	"github.com/hunkvm/locator/server/transport"
	"go.uber.org/zap"

	registrysrv "github.com/hunkvm/locator/pkg/proto/registry"
	transportsrv "github.com/hunkvm/locator/pkg/proto/transport"
	"github.com/hunkvm/locator/pkg/types"

	clustersrv "github.com/envoyproxy/go-control-plane/envoy/service/cluster/v3"
	discoverysrv "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	endpointsrv "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"
	listenersrv "github.com/envoyproxy/go-control-plane/envoy/service/listener/v3"
	routesrv "github.com/envoyproxy/go-control-plane/envoy/service/route/v3"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	cpserver "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"google.golang.org/grpc"
)

type LocatorServer struct {
	logger          *zap.Logger
	config          config.ServerConfig
	locatorEngine   *engine.LocatorEngine
	registryServer  *grpc.Server
	transportServer *grpc.Server
	healthManager   *manager.HealthManager
	snapshotManager *manager.SnapshotManager
}

func NewLocatorServer(cfg config.ServerConfig, logger *zap.Logger) *LocatorServer {
	ctx := context.Background()

	locatorEngine, err := engine.NewLocatorEngine(
		engine.EngineConfig{
			Node: cfg.Node, DataDir: cfg.DataDir,
			Raft: cfg.Raft, Logger: logger,
		},
	)
	if err != nil {
		logger.Fatal("failed to create locator server", zap.Error(err))
	}

	cache := cachev3.NewSnapshotCache(true, cachev3.IDHash{}, nil)
	snapshotManagerCfg := manager.SnapshotManagerConfig{
		Logger:        logger.Named("snapshot"),
		SnapshotCache: cache,
	}
	snapshotManager := manager.NewSnapshotManager(snapshotManagerCfg)
	registryServer, transportServer := grpc.NewServer(), grpc.NewServer()

	discoveryService := cpserver.NewServer(ctx, cache, nil)
	transportService := transport.NewTransportServer(locatorEngine)
	registryService := registry.NewRegistryServer(locatorEngine)

	transportsrv.RegisterRaftTransportServiceServer(transportServer, transportService)
	registrysrv.RegisterServiceRegistryServiceServer(registryServer, registryService)
	discoverysrv.RegisterAggregatedDiscoveryServiceServer(registryServer, discoveryService)
	endpointsrv.RegisterEndpointDiscoveryServiceServer(registryServer, discoveryService)
	clustersrv.RegisterClusterDiscoveryServiceServer(registryServer, discoveryService)
	listenersrv.RegisterListenerDiscoveryServiceServer(registryServer, discoveryService)
	routesrv.RegisterRouteDiscoveryServiceServer(registryServer, discoveryService)

	return &LocatorServer{
		config:          cfg,
		logger:          logger,
		locatorEngine:   locatorEngine,
		registryServer:  registryServer,
		transportServer: transportServer,
		snapshotManager: snapshotManager,
	}
}

func (r *LocatorServer) Start() error {
	healthCallback := manager.HealthManagerCallback{
		OnHealthChanged: func(srv types.Service) {
			r.locatorEngine.Update(context.TODO(), srv)
		},
		OnMonitorStopped: func(srv types.Service) {
			r.locatorEngine.Remove(context.TODO(), srv.ID)
		},
	}

	healthManagerCfg := manager.HealthManagerConfig{
		Logger:                r.logger.Named("health"),
		HealthCheckConfig:     r.config.HealthCheck,
		HealthManagerCallback: healthCallback,
	}

	engineCallback := engine.EngineCallback{
		OnLeaderChanged: func(isLeader bool) {
			if r.healthManager == nil && isLeader {
				r.healthManager = manager.NewHealthManager(healthManagerCfg)
				services, err := r.locatorEngine.Retrieve(
					context.TODO(), nil,
				)
				if err != nil {
					r.logger.Error("failed to monitor services", zap.Error(err))
				}
				for _, srv := range services {
					r.healthManager.StartMonitoring(*srv)
				}
			} else if r.healthManager != nil && !isLeader {
				r.healthManager.TerminateMonitors()
				r.healthManager = nil
			}
		},
		OnServicePersisted: func(srv types.Service) {
			if hm := r.healthManager; hm != nil {
				hm.RestartMonitoring(srv)
			}
		},
		OnServiceRemoved: func(srv types.Service) {
			if hm := r.healthManager; hm != nil {
				hm.StopMonitoring(srv.ID)
			}
			node := srv.Metadata["Node"]
			services, _ := r.locatorEngine.Retrieve(
				context.TODO(), map[string][]any{
					"Metadata.Node": {node},
				},
			)
			r.snapshotManager.UpdateCache(node, services)
		},
		OnServiceUpdated: func(srv types.Service) {
			if srv.HealthStatus == types.HealthStatus_UNSPECIFIED {
				if hm := r.healthManager; hm != nil {
					hm.RestartMonitoring(srv)
				}
			}
			node := srv.Metadata["Node"]
			services, _ := r.locatorEngine.Retrieve(
				context.TODO(), map[string][]any{
					"Metadata.Node": {node},
				},
			)
			r.snapshotManager.UpdateCache(node, services)
		},
		OnSnapshotApplied: func(uint64) { r.refreshCache() },
	}
	r.locatorEngine.Start(engineCallback)
	go r.refreshCache()

	registryAddr := r.config.Registry.Address.String()
	registryListener, err := net.Listen("tcp", registryAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on registration port: %w", err)
	}
	r.logger.Info("service registry listening", zap.String("address", registryAddr))

	transportAddr := r.config.Transport.Address.String()
	transportListener, err := net.Listen("tcp", transportAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on transport port: %w", err)
	}
	r.logger.Info("raft transport listening", zap.String("address", transportAddr))

	errors := make(chan error, 2)
	var waitGroup sync.WaitGroup

	waitGroup.Add(2)
	go func() {
		defer waitGroup.Done()
		if err := r.registryServer.Serve(registryListener); err != nil {
			errors <- fmt.Errorf("failed to serve registry gRPC server: %w", err)
		}
	}()
	go func() {
		defer waitGroup.Done()
		if err := r.transportServer.Serve(transportListener); err != nil {
			errors <- fmt.Errorf("failed to serve transport gRPC server: %w", err)
		}
	}()

	go func() {
		waitGroup.Wait()
		close(errors)
	}()

	return <-errors
}

func (r *LocatorServer) Stop() {
	if r.healthManager != nil {
		r.healthManager.TerminateMonitors()
	}
	r.locatorEngine.Stop()
	r.registryServer.GracefulStop()
	r.transportServer.GracefulStop()
}

func (r *LocatorServer) refreshCache() {
	services, err := r.locatorEngine.Retrieve(context.TODO(), nil)
	if err != nil {
		r.logger.Error("failed to retrieve services for cache refresh", zap.Error(err))
		return
	}

	serviceMap := make(map[string][]*types.Service)
	for _, s := range services {
		node := s.Metadata["Node"]
		if node == "" {
			node = "default"
		}
		serviceMap[node] = append(serviceMap[node], s)
	}

	for node, srvs := range serviceMap {
		r.snapshotManager.UpdateCache(node, srvs)
	}
}
