package engine

type Config struct {
	NodeID    uint64
	PeerAddrs string
	DataDir   string
	Transport TransportConfig
	Registry  RegistryConfig
}

type TransportConfig struct {
	Host string
	Port int
}

type RegistryConfig struct {
	Host string
	Port int
}
