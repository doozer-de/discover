package discover

import (
	consul "github.com/hashicorp/consul/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/naming"
)

type Resolve struct {
	addr string
	tag  string
}

func WithResolver(addr, tag string) grpc.DialOption {
	r := Resolve{addr: addr, tag: tag}
	return grpc.WithBalancer(grpc.RoundRobin(r))
}

// Resolve creates a Watcher for target
func (r Resolve) Resolve(service string) (naming.Watcher, error) {
	cfg := consul.DefaultConfig()
	cfg.Address = r.addr
	conn, err := consul.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	w := &Watch{
		nups: make(chan []*naming.Update),
		errs: make(chan error),
		done: make(chan bool),
	}
	go w.watch(conn, service, r.tag)
	return w, nil
}
