package config

import (
	"time"

	"github.com/hunkvm/locator/pkg/types"
)

type ClientConfig struct {
	Address types.Endpoint
	Timeout time.Duration
}
