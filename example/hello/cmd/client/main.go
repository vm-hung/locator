package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	hello "locator/example/hello/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	_ "google.golang.org/grpc/xds"
)

func main() {
	address := flag.String("address", "localhost:50051", "gRPC server address")
	name := flag.String("name", "World", "Name to greet")
	flag.Parse()

	credentials := insecure.NewCredentials()
	option := grpc.WithTransportCredentials(credentials)
	conn, err := grpc.NewClient(*address, option)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := hello.NewHelloServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"X-Variant", "none",
	))

	r, err := client.SayHello(ctx, &hello.SayHelloRequest{Name: *name})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	fmt.Printf("Greeting: %s\n", r.GetMessage())
}
