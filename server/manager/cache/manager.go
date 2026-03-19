package cache

import (
	"fmt"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	typec "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/hunkvm/locator/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type CacheManager struct {
	logger *zap.Logger
	cache  *ScopedCache
}

func NewCacheManager(cache *ScopedCache, logger *zap.Logger) *CacheManager {
	return &CacheManager{logger: logger, cache: cache}
}

func groupServices(srvs []*types.Service) map[string]map[string][]*types.Service {
	groups := make(map[string]map[string][]*types.Service)
	for _, inst := range srvs {
		name, version := inst.Name, inst.Version
		if _, ok := groups[name]; !ok {
			groups[name] = make(map[string][]*types.Service)
		}
		groups[name][version] = append(groups[name][version], inst)
	}
	return groups
}

func (m *CacheManager) ApplyServices(ns string, srvs []*types.Service) {
	groups := groupServices(srvs)

	clusters := make(map[string]typec.Resource)
	endpoints := make(map[string]typec.Resource)
	routes := make(map[string]typec.Resource)
	listeners := make(map[string]typec.Resource)

	for serviceName, instances := range groups {

		for versionName, insts := range instances {
			clusterName := clusterName(ns, serviceName, versionName)
			endpoints[clusterName] = buildEndpoints(clusterName, insts)
			clusters[clusterName] = buildCluster(clusterName)
		}

		serviceRoutes := buildServiceRoutes(ns, serviceName, instances)
		routeConfigName := fmt.Sprintf("%s-%s-route", ns, serviceName)
		listenerName := fmt.Sprintf("%s-%s-listener", ns, serviceName)
		routes[routeConfigName] = buildRouteConfig(routeConfigName, listenerName, serviceRoutes)
		listeners[listenerName] = buildListener(listenerName, routeConfigName)
	}

	resources := map[string]map[string]typec.Resource{
		resource.ClusterType:  clusters,
		resource.EndpointType: endpoints,
		resource.ListenerType: listeners,
		resource.RouteType:    routes,
	}

	m.cache.ApplyResources(ns, resources)
	m.logger.Debug("applied resources", zap.String("namespace", ns), zap.Any("resources", resources))
}

func (m *CacheManager) UpsertService(ns string, svc *types.Service) {
	cn := clusterName(ns, svc.Name, svc.Version)

	res := m.cache.GetResource(ns, resource.EndpointType, cn)
	if cla, ok := res.(*endpointv3.ClusterLoadAssignment); ok {
		upsertLbEndpoint(cla, svc)
		m.cache.UpdateResource(ns, resource.EndpointType, cn, cla)
		m.logger.Debug("updated endpoint", zap.String("namespace", ns), zap.String("cluster", cn))
		return
	}

	m.cache.UpsertResource(ns, resource.ClusterType, cn, buildCluster(cn))
	m.cache.UpsertResource(ns, resource.EndpointType, cn, buildEndpoints(cn, []*types.Service{svc}))
	m.logger.Debug("created cluster and endpoint", zap.String("namespace", ns), zap.String("cluster", cn))

	rcName := fmt.Sprintf("%s-%s-route", ns, svc.Name)
	listenerName := fmt.Sprintf("%s-%s-listener", ns, svc.Name)

	res = m.cache.GetResource(ns, resource.RouteType, rcName)
	if rc, ok := res.(*routev3.RouteConfiguration); ok {
		upsertClusterInRoute(rc, cn, svc.Version)
		m.cache.UpdateResource(ns, resource.RouteType, rcName, rc)
		m.logger.Debug("updated route config", zap.String("namespace", ns), zap.String("route", rcName))
		return
	}

	serviceRoutes := buildServiceRoutes(ns, svc.Name, map[string][]*types.Service{svc.Version: {svc}})
	m.cache.UpsertResource(ns, resource.RouteType, rcName, buildRouteConfig(rcName, listenerName, serviceRoutes))
	m.cache.UpsertResource(ns, resource.ListenerType, listenerName, buildListener(listenerName, rcName))
	m.logger.Debug("created route and listener", zap.String("namespace", ns),
		zap.String("route", rcName), zap.String("listener", listenerName))
}

func (m *CacheManager) EvictService(ns string, svc *types.Service) {
	cn := clusterName(ns, svc.Name, svc.Version)
	addr := svc.Address.String()
	loc := svc.Metadata.GetLocality()

	res := m.cache.GetResource(ns, resource.EndpointType, cn)
	cla, ok := res.(*endpointv3.ClusterLoadAssignment)
	if !ok {
		return
	}

	for _, ep := range cla.Endpoints {
		if l := ep.GetLocality(); l == nil || l.Region != loc.Region ||
			l.Zone != loc.Zone || l.SubZone != loc.SubZone {
			continue
		}
		n := 0
		for _, lb := range ep.LbEndpoints {
			if a := lb.GetEndpoint().GetAddress().GetSocketAddress(); a != nil {
				if fmt.Sprintf("%s:%d", a.GetAddress(), a.GetPortValue()) == addr {
					continue
				}
			}
			ep.LbEndpoints[n] = lb
			n++
		}
		ep.LbEndpoints = ep.LbEndpoints[:n]
		break
	}

	n := 0
	for _, ep := range cla.Endpoints {
		if len(ep.LbEndpoints) > 0 {
			cla.Endpoints[n] = ep
			n++
		}
	}
	cla.Endpoints = cla.Endpoints[:n]

	m.logger.Debug("removed endpoint", zap.String("namespace", ns),
		zap.String("cluster", cn), zap.String("address", addr))

	if len(cla.Endpoints) > 0 {
		m.cache.UpdateResource(ns, resource.EndpointType, cn, cla)
		return
	}

	m.cache.DeleteResource(ns, resource.EndpointType, cn)
	m.cache.DeleteResource(ns, resource.ClusterType, cn)
	m.logger.Debug("deleted cluster and endpoint", zap.String("namespace", ns),
		zap.String("cluster", cn))

	if m.evictClusterFromRoute(ns, svc.Name, cn) {
		listenerName := fmt.Sprintf("%s-%s-listener", ns, svc.Name)
		m.cache.DeleteResource(ns, resource.ListenerType, listenerName)
		m.logger.Debug("deleted listener", zap.String("namespace", ns),
			zap.String("listener", listenerName))
	}
}

func (m *CacheManager) ClearServices(ns string) {
	m.cache.ClearResources(ns)
}

func (m *CacheManager) evictClusterFromRoute(ns, serviceName, cn string) bool {
	rcName := fmt.Sprintf("%s-%s-route", ns, serviceName)
	res := m.cache.GetResource(ns, resource.RouteType, rcName)
	rc, ok := res.(*routev3.RouteConfiguration)
	if !ok {
		return false
	}

	hasRouteRemains := false
	for _, vh := range rc.VirtualHosts {
		n := 0
		for _, route := range vh.Routes {
			ra, ok := route.Action.(*routev3.Route_Route)
			if !ok {
				vh.Routes[n] = route
				hasRouteRemains = true
				n++
				continue
			}
			switch cs := ra.Route.ClusterSpecifier.(type) {
			case *routev3.RouteAction_Cluster:
				if cs.Cluster == cn {
					continue
				}
				vh.Routes[n] = route
				hasRouteRemains = true
				n++
			case *routev3.RouteAction_WeightedClusters:
				wc := cs.WeightedClusters
				k := 0
				for _, cw := range wc.Clusters {
					if cw.GetName() == cn {
						continue
					}
					wc.Clusters[k] = cw
					k++
				}
				wc.Clusters = wc.Clusters[:k]
				if len(wc.Clusters) == 0 {
					continue
				}
				vh.Routes[n] = route
				hasRouteRemains = true
				n++
			default:
				vh.Routes[n] = route
				hasRouteRemains = true
				n++
			}
		}
		vh.Routes = vh.Routes[:n]
	}

	if !hasRouteRemains {
		m.cache.DeleteResource(ns, resource.RouteType, rcName)
		m.logger.Debug("deleted route config", zap.String("namespace", ns),
			zap.String("route", rcName))
		return true
	}

	m.cache.UpdateResource(ns, resource.RouteType, rcName, rc)
	m.logger.Debug("updated route config", zap.String("namespace", ns),
		zap.String("route", rcName))
	return false
}

func upsertLbEndpoint(cla *endpointv3.ClusterLoadAssignment, svc *types.Service) {
	addr := svc.Address.String()
	loc := svc.Metadata.GetLocality()
	newLocKey := loc.String()

	for epIdx, ep := range cla.Endpoints {
		existingLoc := ep.GetLocality()
		existingLocKey := existingLoc.String()
		for lbIdx, lb := range ep.LbEndpoints {
			if a := lb.GetEndpoint().GetAddress().GetSocketAddress(); a != nil {
				if fmt.Sprintf("%s:%d", a.GetAddress(), a.GetPortValue()) == addr {
					if existingLocKey <= newLocKey {
						return
					}
					ep.LbEndpoints = append(ep.LbEndpoints[:lbIdx], ep.LbEndpoints[lbIdx+1:]...)
					if len(ep.LbEndpoints) == 0 {
						cla.Endpoints = append(cla.Endpoints[:epIdx], cla.Endpoints[epIdx+1:]...)
					}
					break
				}
			}
		}
	}

	for _, ep := range cla.Endpoints {
		l := ep.GetLocality()
		if l == nil || l.String() != newLocKey {
			continue
		}
		ep.LbEndpoints = append(ep.LbEndpoints, newLbEndpoint(svc))
		return
	}

	cla.Endpoints = append(cla.Endpoints, &endpointv3.LocalityLbEndpoints{
		Locality: &corev3.Locality{
			Region:  loc.Region,
			Zone:    loc.Zone,
			SubZone: loc.SubZone,
		},
		LbEndpoints:         []*endpointv3.LbEndpoint{newLbEndpoint(svc)},
		LoadBalancingWeight: wrapperspb.UInt32(100),
	})
}

func newLbEndpoint(svc *types.Service) *endpointv3.LbEndpoint {
	healthStatus := corev3.HealthStatus_UNKNOWN
	switch svc.HealthStatus {
	case types.HealthStatus_UNSPECIFIED:
		healthStatus = corev3.HealthStatus_UNKNOWN
	case types.HealthStatus_HEALTHY:
		healthStatus = corev3.HealthStatus_HEALTHY
	default:
		healthStatus = corev3.HealthStatus_UNHEALTHY
	}
	return &endpointv3.LbEndpoint{
		HostIdentifier: &endpointv3.LbEndpoint_Endpoint{
			Endpoint: &endpointv3.Endpoint{
				Address: &corev3.Address{
					Address: &corev3.Address_SocketAddress{
						SocketAddress: &corev3.SocketAddress{
							Protocol: corev3.SocketAddress_TCP,
							Address:  svc.Address.Host,
							PortSpecifier: &corev3.SocketAddress_PortValue{
								PortValue: uint32(svc.Address.Port),
							},
						},
					},
				},
			},
		},
		HealthStatus:        healthStatus,
		LoadBalancingWeight: wrapperspb.UInt32(1),
	}
}

func upsertClusterInRoute(rc *routev3.RouteConfiguration, cn, version string) {
	for _, vh := range rc.VirtualHosts {
		if version != "" && version != "default" {
			hasHeaderRoute := false
			for _, route := range vh.Routes {
				ra, ok := route.Action.(*routev3.Route_Route)
				if !ok {
					continue
				}
				cs, ok := ra.Route.ClusterSpecifier.(*routev3.RouteAction_Cluster)
				if ok && cs.Cluster == cn {
					hasHeaderRoute = true
					break
				}
			}
			if !hasHeaderRoute {
				headerRoute := &routev3.Route{
					Match: &routev3.RouteMatch{
						PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: "/"},
						Headers: []*routev3.HeaderMatcher{{
							Name: "X-Variant",
							HeaderMatchSpecifier: &routev3.HeaderMatcher_ExactMatch{
								ExactMatch: version,
							},
						}},
					},
					Action: &routev3.Route_Route{
						Route: &routev3.RouteAction{
							ClusterSpecifier: &routev3.RouteAction_Cluster{Cluster: cn},
						},
					},
				}
				// Insert before the catch-all weighted route (always last)
				vh.Routes = append(vh.Routes[:len(vh.Routes)-1],
					append([]*routev3.Route{headerRoute}, vh.Routes[len(vh.Routes)-1:]...)...)
			}
		}

		for _, route := range vh.Routes {
			ra, ok := route.Action.(*routev3.Route_Route)
			if !ok {
				continue
			}
			wcs, ok := ra.Route.ClusterSpecifier.(*routev3.RouteAction_WeightedClusters)
			if !ok {
				continue
			}
			for _, cw := range wcs.WeightedClusters.Clusters {
				if cw.GetName() == cn {
					return
				}
			}
			wcs.WeightedClusters.Clusters = append(wcs.WeightedClusters.Clusters,
				&routev3.WeightedCluster_ClusterWeight{Name: cn, Weight: wrapperspb.UInt32(0)})
			n := uint32(len(wcs.WeightedClusters.Clusters))
			weightPer := uint32(100) / n
			rem := uint32(100) % n
			for i, cw := range wcs.WeightedClusters.Clusters {
				w := weightPer
				if uint32(i) == 0 {
					w += rem
				}
				cw.Weight = wrapperspb.UInt32(w)
			}
			return
		}
	}
}

func clusterName(ns, serviceName, version string) string {
	name := fmt.Sprintf("%s-%s", ns, serviceName)
	if version != "" {
		name += "-" + version
	}
	return name + "-cluster"
}
