package manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hunkvm/locator/server/config"
	"go.uber.org/zap"

	"github.com/hunkvm/locator/pkg/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	defaultMinConnectTimeout  = 5 * time.Second
	defaultMinBackoffDuration = 1 * time.Second
	defaultMaxBackoffDuration = 5 * time.Minute
	defaultMaxBackoffAttempts = 3
)

type HealthManagerCallback struct {
	OnHealthChanged  func(types.Service)
	OnMonitorStopped func(types.Service)
}

type monitorState struct {
	cancel  context.CancelFunc
	restart chan types.Service
}

type HealthManagerConfig struct {
	Logger                *zap.Logger
	HealthCheckConfig     config.HealthCheckConfig
	HealthManagerCallback HealthManagerCallback
}

type HealthManager struct {
	mutex    sync.Mutex
	logger   *zap.Logger
	monitors map[string]*monitorState
	callback HealthManagerCallback

	minConnectTimeout  time.Duration
	minBackoffDuration time.Duration
	maxBackoffDuration time.Duration
	maxBackoffAttempts int
}

func NewHealthManager(cfg HealthManagerConfig) *HealthManager {
	monitors := make(map[string]*monitorState)
	healthCheckConfig := cfg.HealthCheckConfig

	healthManager := &HealthManager{
		logger: cfg.Logger, monitors: monitors,
		callback: cfg.HealthManagerCallback,
	}

	if duration := healthCheckConfig.MinConnectTimeout; duration == 0 {
		healthManager.minConnectTimeout = defaultMinConnectTimeout
	} else {
		healthManager.minConnectTimeout = duration
	}

	if duration := healthCheckConfig.MinBackoffDuration; duration == 0 {
		healthManager.minBackoffDuration = defaultMinBackoffDuration
	} else {
		healthManager.minBackoffDuration = duration
	}

	if duration := healthCheckConfig.MaxBackoffDuration; duration == 0 {
		healthManager.maxBackoffDuration = defaultMaxBackoffDuration
	} else {
		healthManager.maxBackoffDuration = duration
	}

	if attempts := healthCheckConfig.MaxBackoffAttempts; attempts == 0 {
		healthManager.maxBackoffAttempts = defaultMaxBackoffAttempts
	} else {
		healthManager.maxBackoffAttempts = attempts
	}

	return healthManager
}

func (m *HealthManager) StartMonitoring(service types.Service) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, ok := m.monitors[service.ID]; ok {
		m.logger.Info(
			"health monitor already exists",
			zap.String("service", service.ID),
		)
		return
	}

	m.startMonitoringLocked(service)
}

func (m *HealthManager) RestartMonitoring(service types.Service) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if state, ok := m.monitors[service.ID]; ok {
		m.logger.Info(
			"signaling restart health monitor",
			zap.String("service", service.ID),
		)
		state.restart <- service
		return
	}

	m.startMonitoringLocked(service)
}

func (m *HealthManager) StopMonitoring(id string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.stopMonitoringLocked(id)
}

func (m *HealthManager) TerminateMonitors() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for id := range m.monitors {
		m.stopMonitoringLocked(id)
	}
}

func (m *HealthManager) stopMonitoringLocked(id string) {
	if state, exists := m.monitors[id]; exists {
		delete(m.monitors, id)
		state.cancel()
	}
}

func (m *HealthManager) startMonitoringLocked(service types.Service) {
	ctx, cancel := context.WithCancel(context.Background())
	restart := make(chan types.Service, 1)
	m.monitors[service.ID] = &monitorState{
		cancel:  cancel,
		restart: restart,
	}

	go m.startMonitorLoop(ctx, service, restart)
}

func (m *HealthManager) startMonitorLoop(
	ctx context.Context, srv types.Service, restart chan types.Service,
) {
	backoff, attemptsAtMax := m.minBackoffDuration, 0
	resetBackoff := func() {
		backoff = m.minBackoffDuration
		attemptsAtMax = 0
	}

	m.logger.Info(
		"starting monitor service",
		zap.String("service", srv.Name),
		zap.String("address", srv.Address.String()),
	)

	defer func() {
		m.logger.Info(
			"stopped monitoring service",
			zap.String("service", srv.Name),
			zap.String("address", srv.Address.String()),
		)
		if m.callback.OnMonitorStopped != nil {
			m.callback.OnMonitorStopped(srv)
		}
	}()

	for {
		watchDone := make(chan error, 1)
		subCtx, subCancel := context.WithCancel(ctx)

		go func() {
			watchDone <- m.watchServiceHealth(subCtx, srv, resetBackoff)
		}()

		select {
		case <-ctx.Done():
			subCancel()
			return
		case newSrv := <-restart:
			m.logger.Info(
				"restarting monitor service loop",
				zap.String("service", srv.ID),
			)
			srv = newSrv
			subCancel()
			<-watchDone
			resetBackoff()
			continue
		case err := <-watchDone:
			subCancel()
			if err != nil {
				newStatus := types.HealthStatus_UNHEALTHY
				m.logger.Warn(
					"health watch service failed",
					zap.String("service", srv.ID),
					zap.Error(err),
				)

				if srv.HealthStatus != newStatus {
					srv.HealthStatus = newStatus
					if m.callback.OnHealthChanged != nil {
						m.callback.OnHealthChanged(srv)
					}
				}

				if backoff >= m.maxBackoffDuration {
					attemptsAtMax++
					if attemptsAtMax >= m.maxBackoffAttempts {
						m.logger.Warn(
							"max backoff attempts reached, stopping monitor",
							zap.String("service", srv.ID),
						)
						return
					}
				}

				select {
				case <-ctx.Done():
					m.logger.Info(
						"received context cancelled during backoff",
						zap.String("service", srv.ID),
					)
					return
				case newSrv := <-restart:
					m.logger.Info(
						"restarting monitor loop during backoff",
						zap.String("service", srv.ID),
					)
					srv = newSrv
					resetBackoff()
					continue
				case <-time.After(backoff):
					backoff *= 2
					if backoff > m.maxBackoffDuration {
						backoff = m.maxBackoffDuration
					}
				}
			}
		}
	}
}

func (m *HealthManager) watchServiceHealth(
	ctx context.Context, srv types.Service, onConnect func(),
) error {
	credentials := insecure.NewCredentials()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials),
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: m.minConnectTimeout,
		}),
	}

	host, port := srv.Address.Host, srv.Address.Port
	if hc := srv.HealthCheck; hc != nil {
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
	if onConnect != nil {
		onConnect()
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}

		newStatus := types.HealthStatus_UNSPECIFIED
		switch resp.Status {
		case grpc_health_v1.HealthCheckResponse_SERVING:
			newStatus = types.HealthStatus_HEALTHY
		case grpc_health_v1.HealthCheckResponse_NOT_SERVING:
			newStatus = types.HealthStatus_UNHEALTHY
		}

		if srv.HealthStatus != newStatus {
			srv.HealthStatus = newStatus
			m.logger.Info("service health status changed",
				zap.String("service", srv.Name),
				zap.String("address", srv.Address.String()),
				zap.String("status", srv.HealthStatus.String()),
			)
			if m.callback.OnHealthChanged != nil {
				m.callback.OnHealthChanged(srv)
			}
		}
	}
}
