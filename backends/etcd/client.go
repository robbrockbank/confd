package etcd

import (
	"strings"
	"time"

	etcd "github.com/coreos/etcd/client"
	"github.com/coreos/etcd/pkg/transport"
	"golang.org/x/net/context"
)

var (
	clientTimeout = 30 * time.Second
	etcdGetOpts   = &etcd.GetOptions{Quorum: true, Recursive: true}
)

// Client is a wrapper around the etcd client
type Client struct {
	client etcd.Client
	keys   etcd.KeysAPI
}

// NewEtcdClient returns an *etcd.Client with a connection to named machines.
// It returns an error if a connection to the cluster cannot be made.
func NewEtcdClient(machines []string, cert, key string, caCert string, noDiscover bool) (*Client, error) {
	var err error
	machines = prependSchemeToMachines(machines)

	// Create the etcd client
	tls := transport.TLSInfo{
		CAFile:   caCert,
		CertFile: cert,
		KeyFile:  key,
	}
	transport, err := transport.NewTransport(tls, clientTimeout)
	if err != nil {
		return nil, err
	}

	cfg := etcd.Config{
		Endpoints:               machines,
		Transport:               transport,
		HeaderTimeoutPerRequest: clientTimeout,
	}

	client, err := etcd.New(cfg)
	if err != nil {
		return nil, err
	}
	keys := etcd.NewKeysAPI(client)

	return &Client{client: client, keys: keys}, nil
}

// GetValues queries etcd for keys prefixed by prefix.
func (c *Client) GetValues(keys []string) (map[string]string, error) {
	vars := make(map[string]string)
	for _, key := range keys {
		resp, err := c.keys.Get(context.Background(), key, etcdGetOpts)
		if err != nil {
			return vars, err
		}
		err = nodeWalk(resp.Node, vars)
		if err != nil {
			return vars, err
		}
	}
	return vars, nil
}

// nodeWalk recursively descends nodes, updating vars.
func nodeWalk(node *etcd.Node, vars map[string]string) error {
	if node != nil {
		key := node.Key
		if !node.Dir {
			vars[key] = node.Value
		} else {
			for _, node := range node.Nodes {
				nodeWalk(node, vars)
			}
		}
	}
	return nil
}

func (c *Client) WatchPrefix(prefix string, waitIndex uint64, stopChan chan bool, keys []string) (uint64, error) {
	if waitIndex == 0 {
		resp, err := c.keys.Get(context.Background(), prefix, etcdGetOpts)
		if err != nil {
			return 0, err
		}
		return resp.Index, nil
	}

	// Create the watcher.
	watcherOpts := &etcd.WatcherOptions{
		AfterIndex: waitIndex,
		Recursive:  true,
	}
	watcher := c.keys.Watcher(prefix, watcherOpts)
	ctx, cancel := context.WithCancel(context.Background())
	cancelRoutine := make(chan bool)
	defer close(cancelRoutine)
	go func() {
		select {
		case <-stopChan:
			cancel()
		case <-cancelRoutine:
			return
		}
	}()

	for {
		resp, err := watcher.Next(ctx)
		if err != nil {
			switch e := err.(type) {
			case *etcd.Error:
				if e.Code == 401 {
					return 0, nil
				}
			}
			return waitIndex, err
		}

		// Check that the key for this node is one of the keys we care about.
		for _, k := range keys {
			if strings.HasPrefix(resp.Node.Key, k) {
				return resp.Node.ModifiedIndex, err
			}
		}

		waitIndex = resp.Node.ModifiedIndex
	}
}
