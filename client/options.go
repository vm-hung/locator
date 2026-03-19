package client

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/hunkvm/locator/client/config"
	"github.com/hunkvm/locator/pkg/types"
)

type ClientOption func(*config.ClientConfig) error

func WithEndpoint(endpoint types.Endpoint) ClientOption {
	return func(c *config.ClientConfig) error {
		c.Address = endpoint
		return nil
	}
}

func WithAddress(address string) ClientOption {
	return func(c *config.ClientConfig) error {
		if host, port, err := net.SplitHostPort(address); err != nil {
			return fmt.Errorf("%w: %s", ErrInvalidAddress, address)
		} else if portNum, err := strconv.Atoi(port); err != nil {
			return fmt.Errorf("%w: %s", ErrInvalidPortNumber, port)
		} else {
			c.Address = types.Endpoint{Host: host, Port: portNum}
		}
		return nil
	}
}

func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *config.ClientConfig) error {
		if timeout < 0 {
			return fmt.Errorf("%w: %d", ErrInvalidTimeout, timeout)
		}
		c.Timeout = timeout
		return nil
	}
}

func WithConfig(cfg config.ClientConfig) ClientOption {
	return func(c *config.ClientConfig) error {
		*c = cfg
		return nil
	}
}
