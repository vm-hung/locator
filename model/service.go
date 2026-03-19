package model

import (
	"time"
)

type HealthStatus int32

const (
	HealthStatus_UNSPECIFIED HealthStatus = 0
	HealthStatus_HEALTHY     HealthStatus = 1
	HealthStatus_UNHEALTHY   HealthStatus = 2
)

var (
	HealthStatus_name = map[int32]string{
		0: "UNSPECIFIED",
		1: "HEALTHY",
		2: "UNHEALTHY",
	}
	HealthStatus_value = map[string]int32{
		"UNSPECIFIED": 0,
		"HEALTHY":     1,
		"UNHEALTHY":   2,
	}
)

func (s HealthStatus) String() string {
	if name, ok := HealthStatus_name[int32(s)]; ok {
		return name
	}
	return HealthStatus_name[0]
}

type Endpoint struct {
	Host string
	Port int
}

type HealthConfig struct {
	Service string
	Address Endpoint
}

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
