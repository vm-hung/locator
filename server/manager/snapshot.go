package manager

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hunkvm/locator/pkg/types"
	"go.uber.org/zap"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	router "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	v3type "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	v3cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resource "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/wellknown"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/cespare/xxhash/v2"
)

type SnapshotManagerConfig struct {
	Logger        *zap.Logger
	SnapshotCache v3cache.SnapshotCache
}

type SnapshotManager struct {
	mutex    sync.RWMutex
	logger   *zap.Logger
	cache    v3cache.SnapshotCache
	versions map[string]string
}

func NewSnapshotManager(cfg SnapshotManagerConfig) *SnapshotManager {
	return &SnapshotManager{
		logger:   cfg.Logger,
		cache:    cfg.SnapshotCache,
		versions: make(map[string]string),
	}
}

func (m *SnapshotManager) UpdateCache(node string, services []*types.Service) {
	slices.SortFunc(services, func(a, b *types.Service) int {
		return strings.Compare(a.ID, b.ID)
	})

	groups := groupServices(services)

	var clusters []v3type.Resource
	var endpoints []v3type.Resource
	var routes []v3type.Resource
	var listeners []v3type.Resource

	// Sort service names
	serviceNames := make([]string, 0, len(groups))
	for name := range groups {
		serviceNames = append(serviceNames, name)
	}
	sort.Strings(serviceNames)

	for _, serviceName := range serviceNames {
		impls := groups[serviceName]

		// Sort implementation (variant) names
		implNames := make([]string, 0, len(impls))
		for name := range impls {
			implNames = append(implNames, name)
		}
		sort.Strings(implNames)

		for _, implName := range implNames {
			insts := impls[implName]
			clusterName := fmt.Sprintf("%s-%s", serviceName, implName)

			endpoints = append(endpoints, buildEndpoints(clusterName, insts))
			clusters = append(clusters, buildCluster(clusterName))
		}

		serviceRoutes := buildServiceRoutes(serviceName, impls)
		routeConfigName := fmt.Sprintf("%s-route", serviceName)
		routes = append(routes, buildRouteConfig(routeConfigName, serviceName, serviceRoutes))
		listeners = append(listeners, buildListener(serviceName, routeConfigName))
	}

	resources := map[resource.Type][]v3type.Resource{
		resource.ClusterType:  clusters,
		resource.EndpointType: endpoints,
		resource.ListenerType: listeners,
		resource.RouteType:    routes,
	}

	version := computeVersion(resources)

	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.versions[node] == version {
		return
	}

	snapshot, err := v3cache.NewSnapshot(version, resources)
	if err != nil {
		m.logger.Error(
			"failed to create snapshot for node",
			zap.String("node", node), zap.Error(err),
		)
		return
	}

	m.logger.Info(
		"setting snapshot",
		zap.String("node", node),
		zap.String("version", version),
	)
	if err := m.cache.SetSnapshot(context.Background(), node, snapshot); err != nil {
		m.logger.Error(
			"failed to set snapshot for node",
			zap.String("node", node), zap.Error(err),
		)
		return
	}

	m.versions[node] = version
}

func computeVersion(res map[resource.Type][]v3type.Resource) string {
	digest := xxhash.New()
	resourceTypes := []string{
		resource.ClusterType,
		resource.EndpointType,
		resource.ListenerType,
		resource.RouteType,
	}
	for _, rt := range resourceTypes {
		resources := res[rt]
		for _, r := range resources {
			data, _ := proto.Marshal(r)
			digest.Write(data)
		}
	}
	return fmt.Sprintf("%016x", digest.Sum64())
}

func groupServices(services []*types.Service) map[string]map[string][]*types.Service {
	groups := make(map[string]map[string][]*types.Service)
	for _, inst := range services {
		if inst.HealthStatus != types.HealthStatus_HEALTHY ||
			!inst.Enabled {
			continue
		}
		if groups[inst.Name] == nil {
			groups[inst.Name] = make(map[string][]*types.Service)
		}
		variant := inst.Metadata["Variant"]
		if variant == "" {
			variant = "default"
		}
		groups[inst.Name][variant] = append(groups[inst.Name][variant], inst)
	}
	return groups
}

func buildEndpoints(clusterName string, insts []*types.Service) *endpoint.ClusterLoadAssignment {
	var lbEndpoints []*endpoint.LbEndpoint
	seen := make(map[string]bool)

	for _, inst := range insts {
		host, port := inst.Address.Host, inst.Address.Port
		address := fmt.Sprintf("%s:%d", host, port)
		// skip if already seen
		if seen[address] {
			continue
		}
		seen[address] = true

		lbEndpoints = append(lbEndpoints, &endpoint.LbEndpoint{
			HostIdentifier: &endpoint.LbEndpoint_Endpoint{
				Endpoint: &endpoint.Endpoint{
					Address: &core.Address{
						Address: &core.Address_SocketAddress{
							SocketAddress: &core.SocketAddress{
								Protocol: core.SocketAddress_TCP,
								Address:  host,
								PortSpecifier: &core.SocketAddress_PortValue{
									PortValue: uint32(port),
								},
							},
						},
					},
				},
			},
			LoadBalancingWeight: wrapperspb.UInt32(1),
		})
	}

	region, zone := "southeast-asia", "indochina"
	if len(insts) > 0 {
		if r := insts[0].Metadata["Region"]; r != "" {
			region = r
		}
		if z := insts[0].Metadata["Zone"]; z != "" {
			zone = z
		}
	}

	return &endpoint.ClusterLoadAssignment{
		ClusterName: clusterName,
		Endpoints: []*endpoint.LocalityLbEndpoints{{
			Locality: &core.Locality{
				Region: region,
				Zone:   zone,
			},
			LbEndpoints:         lbEndpoints,
			LoadBalancingWeight: wrapperspb.UInt32(100),
		}},
	}
}

func buildCluster(clusterName string) *cluster.Cluster {
	return &cluster.Cluster{
		Name:                 clusterName,
		ConnectTimeout:       durationpb.New(5 * time.Second),
		ClusterDiscoveryType: &cluster.Cluster_Type{Type: cluster.Cluster_EDS},
		EdsClusterConfig: &cluster.Cluster_EdsClusterConfig{
			EdsConfig: &core.ConfigSource{
				ResourceApiVersion: resource.DefaultAPIVersion,
				ConfigSourceSpecifier: &core.ConfigSource_Ads{
					Ads: &core.AggregatedConfigSource{},
				},
			},
		},
		LbPolicy: cluster.Cluster_ROUND_ROBIN,
	}
}

func buildServiceRoutes(serviceName string, impls map[string][]*types.Service) []*route.Route {
	var routes []*route.Route
	var weightedClusters []*route.WeightedCluster_ClusterWeight

	numImpls := uint32(len(impls))
	if numImpls == 0 {
		return routes
	}

	totalWeight := uint32(100)
	weightPerImpl := totalWeight / numImpls
	remainder := totalWeight % numImpls

	i := 0
	for implName := range impls {
		clusterName := fmt.Sprintf("%s-%s", serviceName, implName)

		// Create exact header match route
		if implName != "default" && implName != "" {
			routes = append(routes, &route.Route{
				Match: &route.RouteMatch{
					PathSpecifier: &route.RouteMatch_Prefix{Prefix: "/"},
					Headers: []*route.HeaderMatcher{
						{
							Name: "X-Variant",
							HeaderMatchSpecifier: &route.HeaderMatcher_ExactMatch{
								ExactMatch: implName,
							},
						},
					},
				},
				Action: &route.Route_Route{
					Route: &route.RouteAction{
						ClusterSpecifier: &route.RouteAction_Cluster{
							Cluster: clusterName,
						},
					},
				},
			})
		}

		// Prepare weighted clusters for default fallback route
		weight := weightPerImpl
		if i == 0 {
			weight += remainder
		}
		weightedClusters = append(weightedClusters, &route.WeightedCluster_ClusterWeight{
			Name:   clusterName,
			Weight: wrapperspb.UInt32(weight),
		})
		i++
	}

	// Distribute randomly across all valid clusters if no specific header matches
	if len(weightedClusters) > 0 {
		routes = append(routes, &route.Route{
			Match: &route.RouteMatch{
				PathSpecifier: &route.RouteMatch_Prefix{Prefix: "/"},
			},
			Action: &route.Route_Route{
				Route: &route.RouteAction{
					ClusterSpecifier: &route.RouteAction_WeightedClusters{
						WeightedClusters: &route.WeightedCluster{
							Clusters: weightedClusters,
						},
					},
				},
			},
		})
	}

	return routes
}

func buildRouteConfig(configName string, serviceName string, routes []*route.Route) *route.RouteConfiguration {
	return &route.RouteConfiguration{
		Name: configName,
		VirtualHosts: []*route.VirtualHost{{
			Name:    fmt.Sprintf("%s_vhost", serviceName),
			Domains: []string{"*", serviceName},
			Routes:  routes,
		}},
	}
}

func buildListener(serviceName string, routeConfigName string) *listener.Listener {
	routerConfig, _ := anypb.New(&router.Router{})

	manager := &hcm.HttpConnectionManager{
		CodecType:  hcm.HttpConnectionManager_AUTO,
		StatPrefix: "ingress_http",
		RouteSpecifier: &hcm.HttpConnectionManager_Rds{
			Rds: &hcm.Rds{
				ConfigSource: &core.ConfigSource{
					ResourceApiVersion: resource.DefaultAPIVersion,
					ConfigSourceSpecifier: &core.ConfigSource_Ads{
						Ads: &core.AggregatedConfigSource{},
					},
				},
				RouteConfigName: routeConfigName,
			},
		},
		HttpFilters: []*hcm.HttpFilter{{
			Name: wellknown.Router,
			ConfigType: &hcm.HttpFilter_TypedConfig{
				TypedConfig: routerConfig,
			},
		}},
	}

	pbst, err := anypb.New(manager)
	if err != nil {
		panic(fmt.Errorf("failed to create manager: %w", err))
	}

	// The listener name dictates what xDS proxy clients match against
	return &listener.Listener{
		Name: serviceName,
		ApiListener: &listener.ApiListener{
			ApiListener: pbst,
		},
	}
}
