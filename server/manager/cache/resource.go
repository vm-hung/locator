package cache

import (
	"fmt"
	"sort"
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	routerv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/wellknown"
	"github.com/hunkvm/locator/pkg/types"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func buildCluster(clusterName string) *clusterv3.Cluster {
	return &clusterv3.Cluster{
		Name:                 clusterName,
		ConnectTimeout:       durationpb.New(5 * time.Second),
		ClusterDiscoveryType: &clusterv3.Cluster_Type{Type: clusterv3.Cluster_EDS},
		EdsClusterConfig: &clusterv3.Cluster_EdsClusterConfig{
			EdsConfig: &corev3.ConfigSource{
				ResourceApiVersion: resource.DefaultAPIVersion,
				ConfigSourceSpecifier: &corev3.ConfigSource_Ads{
					Ads: &corev3.AggregatedConfigSource{},
				},
			},
		},
		LbPolicy: clusterv3.Cluster_ROUND_ROBIN,
	}
}

func buildEndpoints(clusterName string, insts []*types.Service) *endpointv3.ClusterLoadAssignment {
	selected := make(map[string]*types.Service)
	for _, inst := range insts {
		addr := inst.Address.String()
		current, ok := selected[addr]
		if !ok || inst.Metadata.GetLocality().String() <
			current.Metadata.GetLocality().String() {
			selected[addr] = inst
		}
	}

	grouped := make(map[string][]*types.Service)
	localities := make(map[string]types.Locality)
	for _, inst := range selected {
		loc := inst.Metadata.GetLocality()
		key := loc.String()
		grouped[key] = append(grouped[key], inst)
		localities[key] = loc
	}

	localityKeys := make([]string, 0, len(grouped))
	for key := range grouped {
		localityKeys = append(localityKeys, key)
	}
	sort.Strings(localityKeys)

	endpoints := make([]*endpointv3.LocalityLbEndpoints, 0, len(localityKeys))
	for _, key := range localityKeys {
		services := grouped[key]
		sort.Slice(services, func(i, j int) bool {
			return services[i].Address.String() < services[j].Address.String()
		})

		lbEndpoints := make([]*endpointv3.LbEndpoint, 0, len(services))
		for _, inst := range services {
			lbEndpoints = append(lbEndpoints, newLbEndpoint(inst))
		}

		loc := localities[key]
		endpoints = append(endpoints, &endpointv3.LocalityLbEndpoints{
			Locality: &corev3.Locality{
				Region:  loc.Region,
				Zone:    loc.Zone,
				SubZone: loc.SubZone,
			},
			LbEndpoints:         lbEndpoints,
			LoadBalancingWeight: wrapperspb.UInt32(100),
		})
	}

	return &endpointv3.ClusterLoadAssignment{
		ClusterName: clusterName,
		Endpoints:   endpoints,
	}
}

func buildServiceRoutes(ns, serviceName string, impls map[string][]*types.Service) []*routev3.Route {
	var routes []*routev3.Route
	var weightedClusters []*routev3.WeightedCluster_ClusterWeight

	numImpls := uint32(len(impls))
	if numImpls == 0 {
		return routes
	}

	totalWeight := uint32(100)
	weightPerImpl := totalWeight / numImpls
	remainder := totalWeight % numImpls

	i := 0
	for implName := range impls {
		clusterName := fmt.Sprintf("%s-%s", ns, serviceName)
		if implName != "" {
			clusterName += "-" + implName
		}
		clusterName += "-cluster"

		// Create exact header match route
		if implName != "default" && implName != "" {
			routes = append(routes, &routev3.Route{
				Match: &routev3.RouteMatch{
					PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: "/"},
					Headers: []*routev3.HeaderMatcher{
						{
							Name: "X-Variant",
							HeaderMatchSpecifier: &routev3.HeaderMatcher_ExactMatch{
								ExactMatch: implName,
							},
						},
					},
				},
				Action: &routev3.Route_Route{
					Route: &routev3.RouteAction{
						ClusterSpecifier: &routev3.RouteAction_Cluster{
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
		weightedClusters = append(weightedClusters, &routev3.WeightedCluster_ClusterWeight{
			Name:   clusterName,
			Weight: wrapperspb.UInt32(weight),
		})
		i++
	}

	// Distribute randomly across all valid clusters if no specific header matches
	if len(weightedClusters) > 0 {
		routes = append(routes, &routev3.Route{
			Match: &routev3.RouteMatch{
				PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: "/"},
			},
			Action: &routev3.Route_Route{
				Route: &routev3.RouteAction{
					ClusterSpecifier: &routev3.RouteAction_WeightedClusters{
						WeightedClusters: &routev3.WeightedCluster{
							Clusters: weightedClusters,
						},
					},
				},
			},
		})
	}

	return routes
}

func buildRouteConfig(configName string, serviceName string, routes []*routev3.Route) *routev3.RouteConfiguration {
	return &routev3.RouteConfiguration{
		Name: configName,
		VirtualHosts: []*routev3.VirtualHost{{
			Name:    fmt.Sprintf("%s_vhost", serviceName),
			Domains: []string{serviceName},
			Routes:  routes,
		}},
	}
}

func buildListener(serviceName string, routeConfigName string) *listenerv3.Listener {
	routerConfig, _ := anypb.New(&routerv3.Router{})

	manager := &hcmv3.HttpConnectionManager{
		CodecType:  hcmv3.HttpConnectionManager_AUTO,
		StatPrefix: "ingress_http",
		RouteSpecifier: &hcmv3.HttpConnectionManager_Rds{
			Rds: &hcmv3.Rds{
				ConfigSource: &corev3.ConfigSource{
					ResourceApiVersion: resource.DefaultAPIVersion,
					ConfigSourceSpecifier: &corev3.ConfigSource_Ads{
						Ads: &corev3.AggregatedConfigSource{},
					},
				},
				RouteConfigName: routeConfigName,
			},
		},
		HttpFilters: []*hcmv3.HttpFilter{{
			Name: wellknown.Router,
			ConfigType: &hcmv3.HttpFilter_TypedConfig{
				TypedConfig: routerConfig,
			},
		}},
	}

	pbst, err := anypb.New(manager)
	if err != nil {
		panic(fmt.Errorf("failed to create manager: %w", err))
	}

	// The listener name dictates what xDS proxy clients match against
	return &listenerv3.Listener{
		Name: serviceName,
		ApiListener: &listenerv3.ApiListener{
			ApiListener: pbst,
		},
	}
}
