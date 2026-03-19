package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"

	"locator/client"
	hello "locator/example/hello/api"
	hellosrv "locator/example/hello/server"
	"locator/model"

	health "google.golang.org/grpc/health/grpc_health_v1"

	healthsrv "google.golang.org/grpc/health"

	"google.golang.org/grpc"
)

func main() {
	address := flag.String("address", ":50051", "gRPC server listen address")
	registry := flag.String("registry", "localhost:8881", "Registry address")
	flag.Parse()

	lis, err := net.Listen("tcp", *address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	healthServer := healthsrv.NewServer()
	helloServer := hellosrv.NewServer()

	health.RegisterHealthServer(grpcServer, healthServer)
	hello.RegisterHelloServiceServer(grpcServer, helloServer)

	healthServer.SetServingStatus("hello", health.HealthCheckResponse_SERVING)

	log.Printf("gRPC server listening on %s\n", *address)

	go registerService(*address, *registry)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func registerService(serviceAddr string, registryAddr string) {
	host, portStr, err := net.SplitHostPort(serviceAddr)
	if err != nil {
		panic(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		panic(err)
	}
	config := client.Config{Address: registryAddr}
	registry := client.NewServiceRegistry(config)
	service := model.Service{
		Name: "hello",
		Address: model.Endpoint{
			Host: host,
			Port: port,
		},
		Enabled: true,
		Metadata: map[string]string{
			"Variant": "none",
			"Version": "v1",
			"Node":    "hello-server",
			"Region":  "southeast-asia",
			"Zone":    "indochina",
		},
	}
	id, err := registry.Register(context.Background(), service)
	if err != nil {
		panic(err)
	}
	fmt.Println("Service registered with ID:", id)
	selectors := []model.Selector{{Field: "ID", Values: []string{id}}}
	services, err := registry.Retrieve(context.Background(), selectors)
	if err != nil {
		panic(err)
	}
	for _, service := range services {
		fmt.Printf("Retrieved service: %+v\n", service)
	}

	log.Printf("Registry address: %s", registryAddr)
}
