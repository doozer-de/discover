// Package discover implements consul dicovery for gRPC
//
// URI <scheme>://<authority>/<endpoint>
//
package discover

import (
	"context"
	"fmt"
	"log"

	"github.com/hashicorp/consul/api"
	"google.golang.org/grpc/resolver"
)

type consulBuilder struct{}

// Register resolver
func init() {
	resolver.Register(&consulBuilder{})
}

// Scheme of resolver
func (c *consulBuilder) Scheme() string {
	return "consul"
}

// Build resover
func (c *consulBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	cfg := api.DefaultConfig()
	cfg.Address = target.Authority
	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &consulResolver{
		target: target,
		conn:   cc,
		cancel: cancel,
		client: client,
		opts:   new(api.QueryOptions).WithContext(ctx),
	}
	go r.start(ctx)
	return r, nil
}

type consulResolver struct {
	target resolver.Target
	conn   resolver.ClientConn
	cancel context.CancelFunc
	client *api.Client
	opts   *api.QueryOptions
	prev   []resolver.Address // previous resolved addresses
}

func (c *consulResolver) healthy() ([]resolver.Address, error) {
	var a []resolver.Address
	se, meta, err := c.client.Health().Service(c.target.Endpoint, "", true, c.opts)
	if err != nil {
		if c.opts.Context().Err() == context.Canceled {
			return nil, nil
		}
		return nil, err
	}
	// setup next wait index for long polling
	c.opts.WaitIndex = meta.LastIndex
	for _, v := range se {
		a = append(a, resolver.Address{
			Addr: fmt.Sprintf("%s:%d", v.Service.Address, v.Service.Port),
			Type: resolver.Backend,
		})
	}
	return a, nil
}

func has(x []resolver.Address, a resolver.Address) bool {
	for _, v := range x {
		if v.Addr == a.Addr {
			return true
		}
	}
	return false
}

func (c *consulResolver) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("connection to %v closed", c.target.Endpoint)
			return
		default:
			c.ResolveNow(resolver.ResolveNowOptions{})
		}
	}
}

func (c *consulResolver) ResolveNow(_ resolver.ResolveNowOptions) {
	curr, err := c.healthy()
	if err != nil {
		log.Println(err)
		return
	}
	c.prev = curr
	c.conn.NewAddress(curr)
}

func (c *consulResolver) Close() {
	log.Printf("closing connection to %v", c.target.Endpoint)
	c.cancel()
}
