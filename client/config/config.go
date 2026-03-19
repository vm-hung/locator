package config

import (
	"time"
)

type ClientConfig struct {
	Address string
	Timeout time.Duration
}
