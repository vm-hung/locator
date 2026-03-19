package manager

import (
	"context"
	"fmt"
	"locator/model"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type HealthManagerCallback struct {
	OnHealthChanged  func(*model.Service)
	OnMonitorStopped func(*model.Service)
}

type HealthManager struct {
	mutex    sync.Mutex
	monitors map[string]context.CancelFunc
	callback HealthManagerCallback
}

func NewHealthManager(callback HealthManagerCallback) *HealthManager {
	monitors := make(map[string]context.CancelFunc)
	return &HealthManager{monitors: monitors, callback: callback}
}

func (hcm *HealthManager) StartMonitoring(server *model.Service) {
	hcm.mutex.Lock()
	defer hcm.mutex.Unlock()

	if _, exists := hcm.monitors[server.ID]; exists {
		log.Printf("Re-registering health monitor for %s", server.ID)
		hcm.StopMonitoring(server.ID)
	}

	ctx, cancel := context.WithCancel(context.Background())
	hcm.monitors[server.ID] = cancel

	go hcm.monitorLoop(ctx, server)
}

func (hcm *HealthManager) StopMonitoring(id string) {
	hcm.mutex.Lock()
	defer hcm.mutex.Unlock()

	if cancel, exists := hcm.monitors[id]; exists {
		cancel()
		delete(hcm.monitors, id)
	}
}

func (hcm *HealthManager) TerminateMonitors() {
	for id := range hcm.monitors {
		hcm.StopMonitoring(id)
	}
}

func (m *HealthManager) monitorLoop(ctx context.Context, service *model.Service) {
	backoff, maxBackoff := 1*time.Second, 5*time.Minute
	maxBackoffAttempts, attemptsAtMax := 3, 0

	resetBackoff := func() {
		log.Printf("Resetting backoff for %s", service.ID)
		backoff = 1 * time.Second
		attemptsAtMax = 0
	}

	format := "Starting monitor for %s (%s)"
	log.Printf(format, service.Name, service.Address)

	defer func() {
		format := "Stopped monitoring %s (%+v)"
		log.Printf(format, service.Name, service.Address)
		m.callback.OnMonitorStopped(service)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			log.Printf("Watching health for %s", service.ID)
			err := m.watchHealth(ctx, service, resetBackoff)
			if err != nil {
				service.HealthStatus = model.HealthStatus_UNHEALTHY
				format := "Health watch failed for %s: %v. Retrying in %v, attempt %d/%d"
				log.Printf(format, service.ID, err, backoff, attemptsAtMax, maxBackoffAttempts)
				m.callback.OnHealthChanged(service)

				if backoff >= maxBackoff {
					attemptsAtMax++
					if attemptsAtMax >= maxBackoffAttempts {
						format = "Max backoff attempts reached for %s. Stopping monitor."
						log.Printf(format, service.ID)
						return
					}
				}

				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}
			}
		}
	}
}

func (hcm *HealthManager) watchHealth(
	ctx context.Context, service *model.Service, onConnect func(),
) error {
	credentials := insecure.NewCredentials()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials),
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: 5 * time.Second,
		}),
	}

	host, port := service.Address.Host, service.Address.Port
	if hc := service.HealthCheck; hc != nil {
		host, port = hc.Address.Host, hc.Address.Port
	}
	address := fmt.Sprintf("%s:%d", host, port)
	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := grpc_health_v1.NewHealthClient(conn)
	req := &grpc_health_v1.HealthCheckRequest{Service: ""}
	stream, err := client.Watch(ctx, req)
	if err != nil {
		return err
	}
	onConnect()

	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}

		log.Printf("Health status for %s changed to %s", service.ID, resp.Status)

		switch resp.Status {
		case grpc_health_v1.HealthCheckResponse_SERVING:
			service.HealthStatus = model.HealthStatus_HEALTHY
		case grpc_health_v1.HealthCheckResponse_NOT_SERVING:
			service.HealthStatus = model.HealthStatus_UNHEALTHY
		case grpc_health_v1.HealthCheckResponse_SERVICE_UNKNOWN:
			service.HealthStatus = model.HealthStatus_UNSPECIFIED
		}

		format := "Health status for %s changed to %s"
		log.Printf(format, service.ID, service.HealthStatus)
		hcm.callback.OnHealthChanged(service)
	}
}
