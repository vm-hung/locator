package client

import (
	"github.com/hunkvm/locator/client/config"
	"github.com/hunkvm/locator/client/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type LocatorClient struct {
	config   config.ClientConfig
	conn     *grpc.ClientConn
	Registry *service.ServiceRegistry
	Member   *service.MemberService
}

func NewLocatorClient(options ...ClientOption) (*LocatorClient, error) {
	config := config.DefaultClientConfig()
	for _, option := range options {
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	return newLocatorClient(config)
}

func (c *LocatorClient) Close() error {
	return c.conn.Close()
}

func newLocatorClient(cfg config.ClientConfig) (*LocatorClient, error) {
	credentials := insecure.NewCredentials()
	transport := grpc.WithTransportCredentials(credentials)
	conn, err := grpc.NewClient(cfg.Address, transport)
	if err != nil {
		return nil, err
	}

	registry := service.NewServiceRegistry(conn, cfg.Timeout)
	member := service.NewMemberService(conn, cfg.Timeout)

	return &LocatorClient{
		config: cfg, conn: conn,
		Registry: registry, Member: member,
	}, nil
}
