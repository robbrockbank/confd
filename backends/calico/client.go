package k8s

import (
	"strings"
	"sync"

	"github.com/kelseyhightower/confd/log"

	"github.com/projectcalico/libcalico-go/lib/backend/api"
	"github.com/projectcalico/libcalico-go/lib/backend"
	"github.com/projectcalico/libcalico-go/lib/apiconfig"
	"github.com/projectcalico/libcalico-go/lib/backend/model"
)

const (
	globalASN      = "/calico/bgp/v1/global/as_num"
	globalNodeMesh = "/calico/bgp/v1/global/node_mesh"
	globalLogging  = "/calico/bgp/v1/global/loglevel"
)

func NewCalicoClient(configfile string) (*client, error) {
	// Load the client config.  This loads from the environment if a filename
	// has not been specified.
	config, err := apiconfig.LoadClientConfig(configfile)
	if err != nil {
		log.Error("Failed to load Calico client configuration: %v", err)
		return nil, err
	}

	bc, err := backend.NewClient(*config)
	if err != nil {
		log.Error("Failed to create Calico client: %v", err)
		return nil, err
	}

	c := &client{
		client: bc,
		cache: make(map[string]string),
		cacheRevision: 0,
	}

	// Increment the waitForSync wait group.  This blocks the GetValues call until the
	// syncer has completed it's initial snapshot and is in sync.
	c.waitForSync.Add(1)

	// Query the

	// Start the main syncer loop.
	c.syncer = bc.Syncer(c)
	c.syncer.Start()

	return c, nil
}

// client implements the StoreClient interface for confd, and also implements the
// Calico api.SyncerCallbacks and api.SyncerParseFailCallbacks interfaces for use by
// the BGP Syncer runs in the background.
type client struct {
	// The Calico backend client.
	client api.Client

	// The BGP syncer.
	syncer api.Syncer

	// Whether we have received the in-sync notification from the syncer.  We cannot
	// start rendering until we are in-sync, so we block calls to GetValues until we
	// have synced.
	synced bool
	waitForSync       sync.WaitGroup

	// Our internal cache of key/values, and our (internally defined) cache revision.
	cache             map[string]string
	cacheRevision     uint64

	// The current revision for each prefix.  A revision is updated when we have a sync
	// event that updates any keys with that prefix.
	revisionsByPrefix map[string]uint64

	// Lock used to synchronize access to any of the shared mutable data.
	cacheLock         sync.Mutex
	watcherCond       sync.Cond
}

func (c *client) syncerLoop() {
	syncer := c.client.Syncer(c)
	syncer.Start()
}

func (c *client) OnStatusUpdated(status api.SyncStatus) {
	// We should only get a single in-sync status update.  When we do, unblock the GetValues
	// calls.
	if status == api.InSync {
		c.cacheLock.Lock()
		defer c.cacheLock.Unlock()
		c.synced = true
		c.waitForSync.Done()
	}
}

func (c *client) OnUpdates(updates []api.Update) {
	// Update our cache from the updates.
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	// If we are in-sync then increment our internal cache revision.
	if c.synced {
		c.cacheRevision++
	}

	// Update our cache from each of the individual updates, and keep track of
	// any of the prefixes that are impacted.
	for _, u := range updates {
		// Update our cache of current entries.
		k, err := model.KeyToDefaultPath(u.Key)
		log.Error("Unable to create path from Key %s: %s", u.Key, err)

		switch u.UpdateType {
		case api.UpdateTypeKVDeleted:
			delete(c.cache, k)
		case api.UpdateTypeKVNew, api.UpdateTypeKVUpdated:
			value, err := model.SerializeValue(&u.KVPair)
			if err != nil {
				log.Error("Unable to serialized value %s: %s", u.KVPair.Value, err)
				continue
			}
			c.cache[k] = string(value)
		}

		if c.synced {
			c.keyUpdated(k)
		}
	}
}

type SyncerParseFailCallbacks interface {
	log.Error("Unable to parse datastore entry Key=%s; Value=%s", rawKey, rawValue)
}

// GetValues takes the etcd like keys and route it to the appropriate k8s API endpoint.
func (c *client) GetValues(keys []string) (map[string]string, error) {
	// We should block GetValues until we have the sync'd notification.  At this point we
	// know we have a complete set of data.
	c.waitForSync.Wait()

	// For simplicity always populate the results with the common set of default values
	// (event if we haven't been asked for them).
	var values = map[string]string{
		globalLogging:  "info",
		globalASN:      "64512",
		globalNodeMesh: `{"enabled": true}`,
	}

	// Lock the data and then populate the results from our cache, selecting the data
	// whose path matches the set of prefix keys.
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()
	for k, v := range c.cache {
		if c.matchesPrefix(k, keys) {
			values[k] = v
		}
	}

	return values, nil
}

// WatchPrefix waits for sync events and checks if any of the key prefixes we are interested in have
// been modified.  Function returns when a key is modified.  We currently do not use the stopChan.
func (c *client) WatchPrefix(prefix string, keys []string, waitIndex uint64, stopChan chan bool) (uint64, error) {
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	if waitIndex == 0 {
		// This is the first watch invocation for this prefix, so start by ensuring all of our keys are being
		// monitored, and then exit with the current cacheRevision to render with the current data.
		for _, key := range keys {
			if _, ok := c.revisionsByPrefix[key]; !ok {
				c.revisionsByPrefix[prefix] = 0
			}
		}
		return c.cacheRevision, nil
	}

	for {
		// Loop through each key, if the revision associated with the key is higher than the waitIndex
		// then exit with the current cacheRevision to render with the current data.
		for _, key := range keys {
			if c.revisionsByPrefix[key] > waitIndex {
				return c.cacheRevision, nil
			}
		}

		// No changes for this watcher, so wait until there are more syncer events.
		c.watcherCond.Wait()
	}
}

// matchesPrefix returns true if the key matches any of the supplied prefixes.
func (c *client) matchesPrefix(key string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// Called when a key is updated.  This updates the revision associated with key prefixes
// affected by this key.
// The caller should be holding the cacheLock.
func (c *client) keyUpdated(key string) {
	for prefix, rev := range c.revisionsByPrefix {
		if rev != c.cacheRevision && strings.HasPrefix(key, prefix) {
			c.revisionsByPrefix[prefix] = rev
		}
	}
}
