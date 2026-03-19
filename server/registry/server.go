package registry

import (
	"context"
	"locator/api/registry"
	"locator/model"
	"locator/server/engine"
	"log"
	"time"

	"github.com/google/uuid"
)

type RegistryServer struct {
	locatorServer *engine.LocatorServer
	registry.UnimplementedServiceRegistryServiceServer
}

func NewRegistryServer(locatorServer *engine.LocatorServer) *RegistryServer {
	return &RegistryServer{locatorServer: locatorServer}
}

func (s *RegistryServer) Retrieve(
	ctx context.Context, req *registry.RetrieveServiceRequest,
) (*registry.RetrieveServiceResponse, error) {
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
		return &registry.RetrieveServiceResponse{}, err
	}
	result := make([]*registry.Service, 0, len(services))

	for _, srv := range services {
		service := &registry.Service{
			Id:   srv.ID,
			Name: srv.Name,
			Address: &registry.Endpoint{
				Host: srv.Address.Host,
				Port: int32(srv.Address.Port),
			},
			Enabled:      srv.Enabled,
			HealthStatus: registry.HealthStatus(srv.HealthStatus),
			Metadata:     srv.Metadata,
			RegisteredAt: srv.RegisteredAt.Format(time.RFC3339Nano),
		}
		if hc := srv.HealthCheck; hc != nil {
			service.HealthCheck = &registry.HealthConfig{
				Service: hc.Service,
				Address: &registry.Endpoint{
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
	return &registry.RetrieveServiceResponse{Services: result}, nil
}

func (s *RegistryServer) Register(
	ctx context.Context, req *registry.RegisterServiceRequest,
) (*registry.RegisterServiceResponse, error) {
	service := model.Service{
		Name: req.Name,
		Address: model.Endpoint{
			Host: req.Address.Host,
			Port: int(req.Address.Port),
		},
		Enabled:  req.Enabled,
		Metadata: req.Metadata,
	}

	if hc := req.HealthCheck; hc != nil {
		service.HealthCheck = &model.HealthConfig{
			Service: hc.Service,
			Address: model.Endpoint{
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
		service.HealthStatus = existing[0].HealthStatus
		if err := s.locatorServer.Update(ctx, &service); err != nil {
			return &registry.RegisterServiceResponse{Success: false}, err
		}
		return &registry.RegisterServiceResponse{Id: service.ID, Success: true}, nil
	} else {
		service.ID = uuid.New().String()
		log.Printf("Assigning new service ID: %s", service.ID)
		if err := s.locatorServer.Create(ctx, &service); err != nil {
			return &registry.RegisterServiceResponse{Success: false}, err
		}
		return &registry.RegisterServiceResponse{Id: service.ID, Success: true}, nil
	}
}

func (s *RegistryServer) Enable(
	ctx context.Context, req *registry.EnableServiceRequest,
) (*registry.EnableServiceResponse, error) {
	return &registry.EnableServiceResponse{}, nil
}

func (s *RegistryServer) Disable(
	ctx context.Context, req *registry.DisableServiceRequest,
) (*registry.DisableServiceResponse, error) {
	return &registry.DisableServiceResponse{}, nil
}

func (s *RegistryServer) Deregister(
	ctx context.Context, req *registry.DeregisterServiceRequest,
) (*registry.DeregisterServiceResponse, error) {
	if err := s.locatorServer.Remove(ctx, req.Id); err != nil {
		return &registry.DeregisterServiceResponse{Success: false}, err
	} else {
		return &registry.DeregisterServiceResponse{Success: true}, nil
	}
}
