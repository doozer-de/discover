package discover

import (
	"fmt"

	consul "github.com/hashicorp/consul/api"

	"google.golang.org/grpc/naming"
)

type Watch struct {
	nups chan []*naming.Update
	errs chan error
	done chan bool
}

// watch performes long polling on changes of healthy services
func (w *Watch) watch(conn *consul.Client, service, tag string) {
	seen := make(map[string]bool)
	qopt := &consul.QueryOptions{}
	for {
		newSeen := make(map[string]bool)
		up := []*naming.Update{}
		srv, qmeta, err := conn.Health().Service(service, tag, true, qopt)
		if err != nil {
			w.errs <- err
			return
		}
		qopt.WaitIndex = qmeta.LastIndex
		for _, v := range srv {
			addr := fmt.Sprintf("%s:%d", v.Service.Address, v.Service.Port)
			// got new entry? add it
			if !seen[addr] {
				up = append(up, &naming.Update{Op: naming.Add, Addr: addr})
			}
			// mark it as seen
			newSeen[addr] = true
			delete(seen, addr)
		}
		// drop entries never seen again
		for addr := range seen {
			up = append(up, &naming.Update{Op: naming.Delete, Addr: addr})
		}
		// update seen for next round
		seen = newSeen
		select {
		case <-w.done:
			return
		default:
			if len(up) > 0 {
				w.nups <- up
			}
		}
	}
}

// Next blocks until an update or error happens. It may return one or more
// updates. The first call should get the full set of the results. It should
// return an error if and onfy if Watcher cannot recover.
func (w *Watch) Next() ([]*naming.Update, error) {
	select {
	case up := <-w.nups:
		return up, nil
	case err := <-w.errs:
		return nil, err
	}
}

// Close watcher
func (w Watch) Close() {
	w.done <- true
}
