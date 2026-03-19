package config

import (
	"time"

	"github.com/hunkvm/locator/pkg/types"
)

func DefaultConfig() ClientConfig {
	return ClientConfig{
		Address: types.Endpoint{
			Host: "127.0.1",
			Port: 8882,
		},
		Timeout: 60 * time.Second,
	}
}
