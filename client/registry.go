package client

import (
	"context"
	"locator/api/registry"
	"locator/model"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ServiceRegistry struct {
	client registry.ServiceRegistryServiceClient
}

func NewServiceRegistry(config Config) *ServiceRegistry {
	credentials := insecure.NewCredentials()
	transport := grpc.WithTransportCredentials(credentials)
	conn, err := grpc.NewClient(config.Address, transport)
	if err != nil {
		panic(err)
	}
	return &ServiceRegistry{
		client: registry.NewServiceRegistryServiceClient(conn),
	}
}

func (r *ServiceRegistry) Retrieve(
	ctx context.Context, selectors []model.Selector,
) ([]*model.Service, error) {
	newSelectors := make([]*registry.Selector, 0, len(selectors))
	for _, selector := range selectors {
		newSelectors = append(newSelectors, &registry.Selector{
			Field:  selector.Field,
			Values: selector.Values,
		})
	}
	req := registry.RetrieveServiceRequest{Selectors: newSelectors}
	res, err := r.client.Retrieve(ctx, &req)
	if err != nil {
		return nil, err
	}
	services := make([]*model.Service, 0, len(res.Services))
	for _, srv := range res.Services {
		service := &model.Service{
			ID:   srv.Id,
			Name: srv.Name,
			Address: model.Endpoint{
				Host: srv.Address.Host,
				Port: int(srv.Address.Port),
			},
			Enabled:  srv.Enabled,
			Metadata: srv.Metadata,
		}
		if hc := srv.HealthCheck; hc != nil {
			service.HealthCheck = &model.HealthConfig{
				Service: hc.Service,
				Address: model.Endpoint{
					Host: hc.Address.Host,
					Port: int(hc.Address.Port),
				},
			}
		}
		services = append(services, service)
	}
	return services, nil
}

func (r *ServiceRegistry) Register(ctx context.Context, service model.Service) (string, error) {
	log.Printf("Registering service: %+v\n", service)
	request := &registry.RegisterServiceRequest{
		Name: service.Name,
		Address: &registry.Endpoint{
			Host: service.Address.Host,
			Port: int32(service.Address.Port),
		},
		Enabled:  service.Enabled,
		Metadata: service.Metadata,
	}
	if hc := service.HealthCheck; hc != nil {
		request.HealthCheck = &registry.HealthConfig{
			Service: hc.Service,
			Address: &registry.Endpoint{
				Host: hc.Address.Host,
				Port: int32(hc.Address.Port),
			},
		}
	}
	response, err := r.client.Register(ctx, request)
	if err != nil {
		return "", err
	}
	log.Printf("Registered service: %+v\n", response)
	return response.Id, nil
}

func (r *ServiceRegistry) Deregister(ctx context.Context, id string) error {
	request := &registry.DeregisterServiceRequest{Id: id}
	_, err := r.client.Deregister(ctx, request)
	return err
}
