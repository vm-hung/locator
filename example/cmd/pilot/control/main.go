package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	router "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	discoverysrv "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	v3type "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	v3cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resource "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	cpserver "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"github.com/envoyproxy/go-control-plane/pkg/wellknown"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// nodeID must match the node.id in the client xDS bootstrap config.
const nodeID = "xds-node"

type serviceEntry struct {
	name string
	host string
	port uint32
}

func main() {
	services := []serviceEntry{
		{name: "alpha", host: "localhost", port: 50061},
		{name: "beta", host: "localhost", port: 50062},
	}

	// ads=false: allow clients to subscribe to individual resource names without
	// needing to subscribe to all resources in the snapshot (required when
	// different gRPC connections target different services within the same snapshot).
	cache := v3cache.NewSnapshotCache(false, v3cache.IDHash{}, nil)

	snap := buildSnapshot(services)
	if err := cache.SetSnapshot(context.Background(), nodeID, snap); err != nil {
		log.Fatalf("set snapshot: %v", err)
	}
	log.Printf("snapshot set for node %q (alpha:50061, beta:50062)", nodeID)

	ctx := context.Background()
	cb := cpserver.CallbackFuncs{
		StreamOpenFunc: func(_ context.Context, id int64, typ string) error {
			log.Printf("[stream %d] opened type=%s", id, typ)
			return nil
		},
		StreamClosedFunc: func(i int64, n *core.Node) {
			log.Printf("[stream %d] closed", i)
		},
		StreamRequestFunc: func(id int64, req *discoverysrv.DiscoveryRequest) error {
			errDetail := ""
			if req.GetErrorDetail() != nil {
				errDetail = " ERROR=" + req.GetErrorDetail().GetMessage()
			}
			log.Printf("[stream %d] request type=%s names=%v node=%s version=%s%s",
				id, req.GetTypeUrl(), req.GetResourceNames(), req.GetNode().GetId(),
				req.GetVersionInfo(), errDetail)
			return nil
		},
		StreamResponseFunc: func(_ context.Context, id int64, _ *discoverysrv.DiscoveryRequest, resp *discoverysrv.DiscoveryResponse) {
			log.Printf("[stream %d] response type=%s nonce=%s resources=%d", id, resp.GetTypeUrl(), resp.GetNonce(), len(resp.GetResources()))
		},
	}
	xdsSrv := cpserver.NewServer(ctx, cache, cb)

	grpcServer := grpc.NewServer()
	discoverysrv.RegisterAggregatedDiscoveryServiceServer(grpcServer, xdsSrv)

	lis, err := net.Listen("tcp", ":18000")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Println("xDS control plane listening on :18000")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func buildSnapshot(services []serviceEntry) *v3cache.Snapshot {
	var clusters, endpoints, routes, listeners []v3type.Resource

	for _, svc := range services {
		clusterName := fmt.Sprintf("%s-default", svc.name)
		routeName := fmt.Sprintf("%s-route", svc.name)

		clusters = append(clusters, makeCluster(clusterName))
		endpoints = append(endpoints, makeEndpoint(clusterName, svc.host, svc.port))
		routes = append(routes, makeRouteConfig(routeName, svc.name, clusterName))
		listeners = append(listeners, makeListener(svc.name, routeName))
	}

	snap, err := v3cache.NewSnapshot("v1", map[resource.Type][]v3type.Resource{
		resource.ClusterType:  clusters,
		resource.EndpointType: endpoints,
		resource.ListenerType: listeners,
		resource.RouteType:    routes,
	})
	if err != nil {
		log.Fatalf("new snapshot: %v", err)
	}
	if err := snap.ConstructVersionMap(); err != nil {
		log.Fatalf("construct version map: %v", err)
	}
	return snap
}

func makeCluster(name string) *cluster.Cluster {
	return &cluster.Cluster{
		Name:                 name,
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

func makeEndpoint(clusterName, host string, port uint32) *endpoint.ClusterLoadAssignment {
	return &endpoint.ClusterLoadAssignment{
		ClusterName: clusterName,
		Endpoints: []*endpoint.LocalityLbEndpoints{{
			Locality: &core.Locality{
				Region:  "local",
				Zone:    "default",
				SubZone: "primary",
			},
			LbEndpoints: []*endpoint.LbEndpoint{{
				HostIdentifier: &endpoint.LbEndpoint_Endpoint{
					Endpoint: &endpoint.Endpoint{
						Address: &core.Address{
							Address: &core.Address_SocketAddress{
								SocketAddress: &core.SocketAddress{
									Protocol: core.SocketAddress_TCP,
									Address:  host,
									PortSpecifier: &core.SocketAddress_PortValue{
										PortValue: port,
									},
								},
							},
						},
					},
				},
				LoadBalancingWeight: wrapperspb.UInt32(1),
			}},
			LoadBalancingWeight: wrapperspb.UInt32(1),
		}},
	}
}

func makeRouteConfig(name, serviceName, clusterName string) *routev3.RouteConfiguration {
	return &routev3.RouteConfiguration{
		Name: name,
		VirtualHosts: []*routev3.VirtualHost{{
			Name:    fmt.Sprintf("%s_vhost", serviceName),
			Domains: []string{serviceName},
			Routes: []*routev3.Route{{
				Match: &routev3.RouteMatch{
					PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: "/"},
				},
				Action: &routev3.Route_Route{
					Route: &routev3.RouteAction{
						ClusterSpecifier: &routev3.RouteAction_Cluster{
							Cluster: clusterName,
						},
					},
				},
			}},
		}},
	}
}

func makeListener(serviceName, routeConfigName string) *listenerv3.Listener {
	routerAny, err := anypb.New(&router.Router{})
	if err != nil {
		log.Fatalf("marshal router: %v", err)
	}

	mgr := &hcm.HttpConnectionManager{
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
				TypedConfig: routerAny,
			},
		}},
	}

	mgrAny, err := anypb.New(mgr)
	if err != nil {
		log.Fatalf("marshal HCM: %v", err)
	}

	return &listenerv3.Listener{
		Name: serviceName,
		ApiListener: &listenerv3.ApiListener{
			ApiListener: mgrAny,
		},
	}
}
