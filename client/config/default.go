package config

import (
	"time"
)

func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		Address: "localhost:8882",
		Timeout: 60 * time.Second,
	}
}
