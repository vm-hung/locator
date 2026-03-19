package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	hello "github.com/hunkvm/locator/example/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	_ "google.golang.org/grpc/xds"
)

func main() {
	addresses := flag.String("addresses", "localhost:50051", "Comma-separated list of gRPC server addresses")
	name := flag.String("name", "World", "Name to greet")
	flag.Parse()

	for i, address := range strings.Split(*addresses, ",") {
		if i > 0 {
			time.Sleep(2 * time.Second)
		}
		if err := callService(address, *name); err != nil {
			log.Printf("error calling %s: %v", address, err)
		}
	}
}

func callService(address, name string) error {
	credentials := insecure.NewCredentials()
	option := grpc.WithTransportCredentials(credentials)
	conn, err := grpc.NewClient(address, option)
	if err != nil {
		return fmt.Errorf("did not connect: %w", err)
	}
	defer conn.Close()

	client := hello.NewHelloServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"X-Variant", "none",
	))

	r, err := client.SayHello(ctx, &hello.SayHelloRequest{Name: name})
	if err != nil {
		return fmt.Errorf("could not greet: %w", err)
	}
	fmt.Printf("Greeting from %s: %s\n", address, r.GetMessage())
	return nil
}
