package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	locator "github.com/hunkvm/locator/client"
	hello "github.com/hunkvm/locator/example/api"
	hellosrv "github.com/hunkvm/locator/example/server"

	"github.com/hunkvm/locator/pkg/types"

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
	client, err := locator.NewLocatorClient(
		locator.WithAddress(registryAddr),
		locator.WithTimeout(5*time.Second),
	)
	if err != nil {
		panic(err)
	}
	service := types.Service{
		Name: "hello",
		Address: types.Endpoint{
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
	id, err := client.Registry.Register(context.Background(), service)
	if err != nil {
		panic(err)
	}
	fmt.Println("Service registered with ID:", id)
	services, err := client.Registry.Retrieve(
		context.Background(),
		map[string][]any{"ID": {id}},
	)
	if err != nil {
		panic(err)
	}
	for _, service := range services {
		fmt.Printf("Retrieved service: %+v\n", service)
	}

	log.Printf("Registry address: %s", registryAddr)
}
