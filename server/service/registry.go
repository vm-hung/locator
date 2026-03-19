package registry

import (
	"context"
	"log"
	"time"

	"github.com/hunkvm/locator/server/engine"

	registrypb "github.com/hunkvm/locator/pkg/proto/registry"
	typespb "github.com/hunkvm/locator/pkg/proto/types"
	"github.com/hunkvm/locator/pkg/types"

	"github.com/google/uuid"
)

type RegistryServer struct {
	locatorServer *engine.LocatorEngine
	registrypb.UnimplementedServiceRegistryServiceServer
}

func NewRegistryServer(locatorServer *engine.LocatorEngine) *RegistryServer {
	return &RegistryServer{locatorServer: locatorServer}
}

func (s *RegistryServer) Retrieve(
	ctx context.Context, req *registrypb.RetrieveServiceRequest,
) (*registrypb.RetrieveServiceResponse, error) {
	selector := make(map[string][]any)
	for _, sel := range req.Selectors {
		if _, ok := selector[sel.Field]; ok {
			selector[sel.Field] = append(selector[sel.Field], sel.Values)
		} else {
			selector[sel.Field] = append([]any{}, sel.Values)
		}
	}
	services, err := s.locatorServer.Retrieve(ctx, selector)
	if err != nil {
		return &registrypb.RetrieveServiceResponse{}, err
	}
	result := make([]*typespb.Service, 0, len(services))

	for _, srv := range services {
		service := &typespb.Service{
			Id:   srv.ID,
			Name: srv.Name,
			Address: &typespb.Endpoint{
				Host: srv.Address.Host,
				Port: int32(srv.Address.Port),
			},
			Enabled:      srv.Enabled,
			HealthStatus: typespb.HealthStatus(srv.HealthStatus),
			Metadata:     srv.Metadata,
			RegisteredAt: srv.RegisteredAt.Format(time.RFC3339Nano),
		}
		if hc := srv.HealthCheck; hc != nil {
			service.HealthCheck = &typespb.HealthConfig{
				Service: hc.Service,
				Address: &typespb.Endpoint{
					Host: hc.Address.Host,
					Port: int32(hc.Address.Port),
				},
			}
		}
		if t := srv.LastChecked; t != nil {
			service.LastChecked = t.Format(time.RFC3339Nano)
		}
		result = append(result, service)
	}
	return &registrypb.RetrieveServiceResponse{Services: result}, nil
}

func (s *RegistryServer) Register(
	ctx context.Context, req *registrypb.RegisterServiceRequest,
) (*registrypb.RegisterServiceResponse, error) {
	service := types.Service{
		Name: req.Name,
		Address: types.Endpoint{
			Host: req.Address.Host,
			Port: int(req.Address.Port),
		},
		Enabled:  req.Enabled,
		Metadata: req.Metadata,
	}

	if hc := req.HealthCheck; hc != nil {
		service.HealthCheck = &types.HealthConfig{
			Service: hc.Service,
			Address: types.Endpoint{
				Host: hc.Address.Host,
				Port: int(hc.Address.Port),
			},
		}
	}

	existing, err := s.locatorServer.Retrieve(
		ctx, map[string][]any{
			"Address.Host":    {req.Address.Host},
			"Address.Port":    {int(req.Address.Port)},
			"Metadata.Node":   {req.Metadata["Node"]},
			"Metadata.Region": {req.Metadata["Region"]},
			"Metadata.Zone":   {req.Metadata["Zone"]},
		},
	)
	if err == nil && len(existing) > 0 {
		service.ID = existing[0].ID
		service.RegisteredAt = existing[0].RegisteredAt
		service.LastChecked = existing[0].LastChecked
		if err := s.locatorServer.Update(ctx, service); err != nil {
			return &registrypb.RegisterServiceResponse{Success: false}, err
		}
		return &registrypb.RegisterServiceResponse{Id: service.ID, Success: true}, nil
	} else {
		service.ID = uuid.New().String()
		log.Printf("Assigning new service ID: %s", service.ID)
		if err := s.locatorServer.Create(ctx, service); err != nil {
			return &registrypb.RegisterServiceResponse{Success: false}, err
		}
		return &registrypb.RegisterServiceResponse{Id: service.ID, Success: true}, nil
	}
}

func (s *RegistryServer) Enable(
	ctx context.Context, req *registrypb.EnableServiceRequest,
) (*registrypb.EnableServiceResponse, error) {
	return &registrypb.EnableServiceResponse{}, nil
}

func (s *RegistryServer) Disable(
	ctx context.Context, req *registrypb.DisableServiceRequest,
) (*registrypb.DisableServiceResponse, error) {
	return &registrypb.DisableServiceResponse{}, nil
}

func (s *RegistryServer) Deregister(
	ctx context.Context, req *registrypb.DeregisterServiceRequest,
) (*registrypb.DeregisterServiceResponse, error) {
	if err := s.locatorServer.Remove(ctx, req.Id); err != nil {
		return &registrypb.DeregisterServiceResponse{Success: false}, err
	} else {
		return &registrypb.DeregisterServiceResponse{Success: true}, nil
	}
}
