package main

import (
	"context"
	"fmt"
	"log"
	"time"

	hello "github.com/hunkvm/locator/example/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/xds"
	_ "google.golang.org/grpc/xds" // register xds resolver
)

const bootstrapConfig = `{
  "xds_servers": [
    {
      "server_uri": "passthrough:///localhost:18000",
      "channel_creds": [{"type": "insecure"}],
      "server_features": ["xds_v3", "trusted_xds_server"]
    }
  ],
  "node": {
    "id": "xds-node",
    "cluster": "test-cluster"
  }
}`

func main() {
	xdsResolver, err := xds.NewXDSResolverWithConfigForTesting(
		[]byte(bootstrapConfig),
	)
	if err != nil {
		log.Fatalf("create xds resolver: %v", err)
	}
	resolverOpt := grpc.WithResolvers(xdsResolver)

	services := []string{"alpha", "beta"}

	// Create all connections upfront so the global XDSClient singleton
	// is shared — both subscriptions go over the same ADS stream.
	conns := make(map[string]*grpc.ClientConn, len(services))
	for _, svc := range services {
		addr := fmt.Sprintf("xds:///%s", svc)
		conn, err := grpc.NewClient(
			addr, resolverOpt,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatalf("connect %s: %v", addr, err)
		}
		conns[svc] = conn

		// Force connection to trigger xDS subscription
		conn.Connect()
	}

	// Close all connections once all calls are done.
	defer func() {
		for _, conn := range conns {
			conn.Close()
		}
	}()

	// Call services sequentially, reusing the pre-created connections.
	for _, svc := range services {
		time.Sleep(2 * time.Second)
		if err := call(conns[svc], "World"); err != nil {
			log.Printf("error calling %s: %v", svc, err)
		}
	}
}

func call(conn *grpc.ClientConn, name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r, err := hello.NewHelloServiceClient(conn).SayHello(
		ctx, &hello.SayHelloRequest{Name: name},
	)
	if err != nil {
		return fmt.Errorf("SayHello: %w", err)
	}
	log.Printf("%s → %s", conn.Target(), r.GetMessage())
	return nil
}
