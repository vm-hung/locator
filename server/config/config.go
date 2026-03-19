package config

import (
	"time"

	"github.com/hunkvm/locator/pkg/types"
)

type ServerConfig struct {
	Node        uint64
	DataDir     string
	Raft        RaftConfig
	Transport   TransportConfig
	Registry    RegistryConfig
	HealthCheck HealthCheckConfig
	Logger      LogConfig
}

type RaftConfig struct {
	PeerAddrs      string
	ElectionTick   int
	Heartbeat      time.Duration
	SnapshotCount  uint64
	CatchupEntries uint64
}

type HealthCheckConfig struct {
	MinBackoffDuration time.Duration
	MaxBackoffDuration time.Duration
	MinConnectTimeout  time.Duration
	MaxBackoffAttempts int
}

type TransportConfig struct {
	Address types.Endpoint
}

type RegistryConfig struct {
	Address types.Endpoint
}

type LogConfig struct {
	Level  string
	Format string
}
