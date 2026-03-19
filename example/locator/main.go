package main

import (
	"flag"
	"locator/server"
	"locator/server/engine"
	"log"
)

func main() {
	id := flag.Uint64("id", 0, "Node ID")
	peers := flag.String("peers", "", "Peer addresses")
	transportHost := flag.String("transport-host", "", "Transport host")
	transportPort := flag.Int("transport-port", 0, "Transport port")
	registryHost := flag.String("registry-host", "", "Registry host")
	registryPort := flag.Int("registry-port", 0, "Registry port")
	dataDir := flag.String("data-dir", "", "Data directory")

	flag.Parse()

	if *id == 0 || *peers == "" || *transportHost == "" ||
		*transportPort == 0 || *registryHost == "" ||
		*registryPort == 0 || *dataDir == "" {
		flag.Usage()
		log.Fatalf("All flags are required")
	}

	config := engine.Config{
		NodeID:    *id,
		PeerAddrs: *peers,
		Transport: engine.TransportConfig{
			Host: *transportHost,
			Port: *transportPort,
		},
		Registry: engine.RegistryConfig{
			Host: *registryHost,
			Port: *registryPort,
		},
		DataDir: *dataDir,
	}
	registry := server.NewServiceRegistry(config)
	if err := registry.Start(); err != nil {
		log.Fatalf("Failed to start service registry: %v", err)
	}
}
