package types

import "time"

const (
	DefaultNode   = "default"
	DefaultRegion = "default"
	DefaultZone   = "default"
)

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
