package manager

import (
	"context"
	"fmt"
	"locator/model"
	"log"
	"sync/atomic"
	"time"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	router "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	v3cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resource "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/wellknown"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type SnapshotManager struct {
	version int64
	cache   v3cache.SnapshotCache
}

func NewSnapshotManager(cache v3cache.SnapshotCache) *SnapshotManager {
	return &SnapshotManager{cache: cache}
}

func (sm *SnapshotManager) UpdateCache(services []*model.Service) {
	servicesByNode := make(map[string][]*model.Service)
	for _, inst := range services {
		if inst.HealthStatus != model.HealthStatus_HEALTHY ||
			!inst.Enabled {
			continue
		}
		nodeID := inst.Metadata["Node"]
		if nodeID == "" {
			nodeID = "default"
		}
		servicesByNode[nodeID] = append(servicesByNode[nodeID], inst)
	}

	version := atomic.AddInt64(&sm.version, 1)
	versionStr := fmt.Sprintf("%d", version)

	// Set cache for each node separately
	for nodeID, nodeServices := range servicesByNode {
		groups := groupServices(nodeServices)

		var clusters []types.Resource
		var endpoints []types.Resource
		var routes []types.Resource
		var listeners []types.Resource

		for serviceName, impls := range groups {
			for implName, insts := range impls {
				clusterName := fmt.Sprintf("%s-%s", serviceName, implName)

				endpoints = append(endpoints, buildEndpoints(clusterName, insts))
				clusters = append(clusters, buildCluster(clusterName))
			}

			serviceRoutes := buildServiceRoutes(serviceName, impls)
			routeConfigName := fmt.Sprintf("%s-route", serviceName)
			routes = append(routes, buildRouteConfig(routeConfigName, serviceName, serviceRoutes))
			listeners = append(listeners, buildListener(serviceName, routeConfigName))
		}

		snapshot, err := v3cache.NewSnapshot(versionStr, map[resource.Type][]types.Resource{
			resource.ClusterType:  clusters,
			resource.EndpointType: endpoints,
			resource.ListenerType: listeners,
			resource.RouteType:    routes,
		})

		if err != nil {
			log.Printf("Failed to create snapshot for node %s: %v", nodeID, err)
			continue
		}

		log.Printf("Setting snapshot for node %s", nodeID)
		err = sm.cache.SetSnapshot(context.Background(), nodeID, snapshot)
		if err != nil {
			log.Printf("Failed to set snapshot for node %s: %v", nodeID, err)
		}
	}
}

func groupServices(services []*model.Service) map[string]map[string][]*model.Service {
	groups := make(map[string]map[string][]*model.Service)
	for _, inst := range services {
		if groups[inst.Name] == nil {
			groups[inst.Name] = make(map[string][]*model.Service)
		}
		variant := inst.Metadata["Variant"]
		if variant == "" {
			variant = "default"
		}
		groups[inst.Name][variant] = append(groups[inst.Name][variant], inst)
	}
	return groups
}

func buildEndpoints(clusterName string, insts []*model.Service) *endpoint.ClusterLoadAssignment {
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

func buildServiceRoutes(serviceName string, impls map[string][]*model.Service) []*route.Route {
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
		log.Fatalf("failed to create manager: %v", err)
	}

	// The listener name dictates what xDS proxy clients match against
	return &listener.Listener{
		Name: serviceName,
		ApiListener: &listener.ApiListener{
			ApiListener: pbst,
		},
	}
}
