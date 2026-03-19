package server

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/hunkvm/locator/server/config"
	"github.com/hunkvm/locator/server/engine"
	"github.com/hunkvm/locator/server/manager/cache"
	"github.com/hunkvm/locator/server/manager/health"
	"github.com/hunkvm/locator/server/service"
	tlspkg "github.com/hunkvm/locator/server/tls"
	"go.uber.org/zap"

	registrysrv "github.com/hunkvm/locator/pkg/proto/registry"
	transportsrv "github.com/hunkvm/locator/pkg/proto/transport"
	"github.com/hunkvm/locator/pkg/types"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoverysrv "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	cpserver "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"google.golang.org/grpc"
)

type LocatorServer struct {
	logger          *zap.Logger
	config          config.ServerConfig
	locatorEngine   *engine.LocatorEngine
	registryServer  *grpc.Server
	transportServer *grpc.Server
	healthManager   *health.HealthManager
	cacheManager    *cache.CacheManager
	registryLoader  *tlspkg.Loader
	transportLoader *tlspkg.Loader
}

func NewLocatorServer(cfg config.ServerConfig, logger *zap.Logger) *LocatorServer {
	ctx := context.Background()

	hybridCache := cache.NewScopedCache(cache.NodeHash{}, logger)
	snapshotManager := cache.NewCacheManager(
		hybridCache, logger.Named("cache-manager"),
	)

	callback := cpserver.CallbackFuncs{
		StreamOpenFunc: func(ctx context.Context, id int64, typeURL string) error {
			logger.Info("stream opened", zap.Int64("id", id), zap.String("type_url", typeURL))
			return nil
		},
		StreamClosedFunc: func(id int64, node *corev3.Node) {
			logger.Info("stream closed", zap.Int64("id", id), zap.Any("node", node))
		},
		StreamRequestFunc: func(id int64, req *discoveryv3.DiscoveryRequest) error {
			logger.Info("stream request", zap.Int64("id", id), zap.Any("request", req))
			return nil
		},
		StreamResponseFunc: func(ctx context.Context, id int64, req *discoveryv3.DiscoveryRequest, resp *discoveryv3.DiscoveryResponse) {
			logger.Info("stream response", zap.Int64("id", id), zap.Any("request", req), zap.Any("response", resp))
		},
		DeltaStreamOpenFunc: func(ctx context.Context, id int64, typeURL string) error {
			logger.Info("delta stream opened", zap.Int64("id", id), zap.String("type_url", typeURL))
			return nil
		},
		DeltaStreamClosedFunc: func(id int64, node *corev3.Node) {
			logger.Info("delta stream closed", zap.Int64("id", id), zap.Any("node", node))
		},
		StreamDeltaRequestFunc: func(id int64, update *discoveryv3.DeltaDiscoveryRequest) error {
			logger.Info("stream delta request", zap.Int64("id", id), zap.Any("update", update))
			return nil
		},
		StreamDeltaResponseFunc: func(id int64, req *discoveryv3.DeltaDiscoveryRequest, resp *discoveryv3.DeltaDiscoveryResponse) {
			logger.Info("stream delta response", zap.Int64("id", id), zap.Any("request", req), zap.Any("response", resp))
		},
		FetchRequestFunc: func(ctx context.Context, req *discoveryv3.DiscoveryRequest) error {
			logger.Info("fetch request", zap.Any("request", req))
			return nil
		},
		FetchResponseFunc: func(req *discoveryv3.DiscoveryRequest, resp *discoveryv3.DiscoveryResponse) {
			logger.Info("fetch response", zap.Any("request", req), zap.Any("response", resp))
		},
	}

	var registryLoader, transportLoader *tlspkg.Loader

	var registryOpts, transportOpts []grpc.ServerOption
	if cfg.Registry.TLS.Enabled {
		rl, err := tlspkg.New(cfg.Registry.TLS, logger.Named("tls-registry"))
		if err != nil {
			logger.Fatal("failed to initialise registry TLS", zap.Error(err))
		}
		registryLoader = rl
		registryOpts = append(registryOpts, grpc.Creds(rl.ServerCredentials()))
	}
	if cfg.Transport.TLS.Enabled {
		tl, err := tlspkg.New(cfg.Transport.TLS, logger.Named("tls-transport"))
		if err != nil {
			logger.Fatal("failed to initialise transport TLS", zap.Error(err))
		}
		transportLoader = tl
		transportOpts = append(transportOpts, grpc.Creds(tl.ServerCredentials()))
	}

	registryServer := grpc.NewServer(registryOpts...)
	transportServer := grpc.NewServer(transportOpts...)

	locatorEngine, err := engine.NewLocatorEngine(
		engine.EngineConfig{
			Node: cfg.Node, DataDir: cfg.DataDir,
			Raft: cfg.Raft, Logger: logger,
			TransportTLSLoader: transportLoader,
		},
	)
	if err != nil {
		logger.Fatal("failed to create locator server", zap.Error(err))
	}

	transportService := service.NewTransportServer(locatorEngine)
	registryService := service.NewRegistryServer(locatorEngine)
	discoveryService := cpserver.NewServer(ctx, hybridCache, callback)

	transportsrv.RegisterRaftTransportServiceServer(transportServer, transportService)
	registrysrv.RegisterServiceRegistryServiceServer(registryServer, registryService)
	discoverysrv.RegisterAggregatedDiscoveryServiceServer(registryServer, discoveryService)

	return &LocatorServer{
		config:          cfg,
		logger:          logger,
		locatorEngine:   locatorEngine,
		registryServer:  registryServer,
		transportServer: transportServer,
		cacheManager:    snapshotManager,
		registryLoader:  registryLoader,
		transportLoader: transportLoader,
	}
}

func (r *LocatorServer) Start() error {
	healthCallback := health.HealthManagerCallback{
		OnHealthChanged: func(srv types.Service) {
			r.locatorEngine.Update(context.TODO(), srv)
		},
		OnMonitorStopped: func(srv types.Service) {
			r.locatorEngine.Remove(context.TODO(), srv.ID)
		},
	}

	healthManagerCfg := health.HealthManagerConfig{
		Logger:                r.logger.Named("health"),
		HealthCheckConfig:     r.config.HealthCheck,
		HealthManagerCallback: healthCallback,
	}

	engineCallback := engine.EngineCallback{
		OnLeaderChanged: func(isLeader bool) {
			if r.healthManager == nil && isLeader {
				r.healthManager = health.NewHealthManager(healthManagerCfg)
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
			ns := srv.Metadata.GetNamespace()
			r.cacheManager.EvictService(ns, &srv)
		},
		OnServiceUpdated: func(srv types.Service) {
			if srv.HealthStatus == types.HealthStatus_UNSPECIFIED {
				if hm := r.healthManager; hm != nil {
					hm.RestartMonitoring(srv)
				}
			}
			ns := srv.Metadata.GetNamespace()
			r.cacheManager.UpsertService(ns, &srv)
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
	if r.registryLoader != nil {
		r.registryLoader.Stop()
	}
	if r.transportLoader != nil {
		r.transportLoader.Stop()
	}
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
		key := s.Metadata.GetNamespace()
		serviceMap[key] = append(serviceMap[key], s)
	}

	for ns, srvs := range serviceMap {
		r.cacheManager.ApplyServices(ns, srvs)
	}
}
