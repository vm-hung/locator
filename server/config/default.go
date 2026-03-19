package config

import "github.com/hunkvm/locator/pkg/types"

func defaultServerConfig() ServerConfig {
	return ServerConfig{
		Node:    1,
		DataDir: "/var/lib/locator",
		Transport: TransportConfig{
			Address: types.Endpoint{
				Host: "127.0.0.1",
				Port: 9991,
			},
		},
		Registry: RegistryConfig{
			Address: types.Endpoint{
				Host: "127.0.0.1",
				Port: 8881,
			},
		},
		Logger: LogConfig{
			Level:  "info",
			Format: "text",
		},
	}
}
