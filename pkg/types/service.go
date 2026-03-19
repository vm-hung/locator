package types

import "time"

type Service struct {
	ID           string
	Name         string
	Address      Endpoint
	HealthStatus HealthStatus
	HealthCheck  *HealthConfig
	Enabled      bool
	Metadata     map[string]string
	LastChecked  *time.Time
	RegisteredAt time.Time
}
