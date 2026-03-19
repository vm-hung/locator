package server

import (
	"context"
	"fmt"
	registrysrv "locator/api/registry"
	transportsrv "locator/api/transport"
	"locator/model"
	"locator/server/engine"
	"locator/server/manager"
	"locator/server/registry"
	"locator/server/transport"
	"log"
	"net"

	clustersrv "github.com/envoyproxy/go-control-plane/envoy/service/cluster/v3"
	discoverysrv "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	endpointsrv "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"
	listenersrv "github.com/envoyproxy/go-control-plane/envoy/service/listener/v3"
	routesrv "github.com/envoyproxy/go-control-plane/envoy/service/route/v3"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	cpserver "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"google.golang.org/grpc"
)

type ServiceRegistry struct {
	config              engine.Config
	locatorServer       *engine.LocatorServer
	registryGrpcServer  *grpc.Server
	transportGrpcServer *grpc.Server
	healthManager       *manager.HealthManager
	snapshotManager     *manager.SnapshotManager
}

func NewServiceRegistry(config engine.Config) *ServiceRegistry {
	ctx := context.Background()
	locatorServer, err := engine.NewLocatorServer(config)
	if err != nil {
		log.Fatalf("Failed to create locator server: %v", err)
	}

	registryGrpcServer, transportGrpcServer := grpc.NewServer(), grpc.NewServer()

	cache := cachev3.NewSnapshotCache(true, cachev3.IDHash{}, nil)
	snapshotManager := manager.NewSnapshotManager(cache)

	discoveryService := cpserver.NewServer(ctx, cache, nil)
	transportService := transport.NewTransportServer(locatorServer)
	registryService := registry.NewRegistryServer(locatorServer)

	transportsrv.RegisterRaftTransportServiceServer(transportGrpcServer, transportService)
	registrysrv.RegisterServiceRegistryServiceServer(registryGrpcServer, registryService)
	discoverysrv.RegisterAggregatedDiscoveryServiceServer(registryGrpcServer, discoveryService)
	endpointsrv.RegisterEndpointDiscoveryServiceServer(registryGrpcServer, discoveryService)
	clustersrv.RegisterClusterDiscoveryServiceServer(registryGrpcServer, discoveryService)
	listenersrv.RegisterListenerDiscoveryServiceServer(registryGrpcServer, discoveryService)
	routesrv.RegisterRouteDiscoveryServiceServer(registryGrpcServer, discoveryService)

	return &ServiceRegistry{
		config:              config,
		locatorServer:       locatorServer,
		registryGrpcServer:  registryGrpcServer,
		transportGrpcServer: transportGrpcServer,
		snapshotManager:     snapshotManager,
	}
}

func (r *ServiceRegistry) Start() error {
	healthCallback := manager.HealthManagerCallback{
		OnHealthChanged: func(service *model.Service) {
			r.locatorServer.Update(context.TODO(), service)
		},
		OnMonitorStopped: func(service *model.Service) {
			r.locatorServer.Remove(context.TODO(), service.ID)
		},
	}
	locatorCallback := engine.LocatorServerCallback{
		OnLeaderChanged: func(isLeader bool) {
			if r.healthManager == nil && isLeader {
				r.healthManager = manager.NewHealthManager(healthCallback)
			} else if r.healthManager != nil && !isLeader {
				r.healthManager.TerminateMonitors()
				r.healthManager = nil
			}
		},
		OnServicePersisted: func(service *model.Service) {
			if hm := r.healthManager; hm != nil {
				hm.StartMonitoring(service)
			}
		},
		OnServiceRemoved: func(service *model.Service) {
			if hm := r.healthManager; hm != nil {
				hm.StopMonitoring(service.ID)
			}
			services, _ := r.locatorServer.Retrieve(
				context.TODO(), map[string][]any{
					"Metadata.Node": {service.Metadata["Node"]},
				},
			)
			r.snapshotManager.UpdateCache(services)
		},
		OnServiceUpdated: func(service *model.Service) {
			services, _ := r.locatorServer.Retrieve(
				context.TODO(), map[string][]any{
					"Metadata.Node": {service.Metadata["Node"]},
				},
			)
			r.snapshotManager.UpdateCache(services)
		},
	}
	r.locatorServer.Start(locatorCallback)

	registryListener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", r.config.Registry.Host, r.config.Registry.Port))
	if err != nil {
		return fmt.Errorf("failed to listen on registration port: %w", err)
	}

	log.Printf("Service Registry listening on %s:%d", r.config.Registry.Host, r.config.Registry.Port)
	go func() {
		if err := r.registryGrpcServer.Serve(registryListener); err != nil {
			log.Fatalf("failed to serve gRPC server: %v", err)
		}
	}()

	transportListener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", r.config.Transport.Host, r.config.Transport.Port))
	if err != nil {
		return fmt.Errorf("failed to listen on transport port: %w", err)
	}

	log.Printf("Raft Transport listening on %s:%d", r.config.Transport.Host, r.config.Transport.Port)
	if err := r.transportGrpcServer.Serve(transportListener); err != nil {
		return fmt.Errorf("failed to serve gRPC server: %w", err)
	}

	return nil
}
