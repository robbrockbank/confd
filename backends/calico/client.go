package calico

import (
	"strings"
	"sync"
	"os"

	"github.com/kelseyhightower/confd/log"

	"github.com/projectcalico/libcalico-go/lib/backend/api"
	"github.com/projectcalico/libcalico-go/lib/backend"
	"github.com/projectcalico/libcalico-go/lib/apiconfig"
	"github.com/projectcalico/libcalico-go/lib/backend/model"
	"github.com/projectcalico/libcalico-go/lib/backend/syncersv1/bgpsyncer"
)

const (
	// Handle a few keys that
	globalASN      = "/calico/bgp/v1/global/as_num"
	globalNodeMesh = "/calico/bgp/v1/global/node_mesh"
	globalLogging  = "/calico/bgp/v1/global/loglevel"
	allNodesKey    = "/calico/bgp/v1/host"
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

	// Create the client.  Initialize the cache revision to 1 so that the watcher
	// code can handle the first iteration by always rendering.
	c := &client{
		client: bc,
		cache: make(map[string]string),
		cacheRevision: 1,
		revisionsByPrefix: make(map[string]uint64),
	}

	// Create a conditional that we use to wake up all of the watcher threads when there
	// may some actionable updates.
	c.watcherCond = sync.NewCond(&c.cacheLock)

	// Increment the waitForSync wait group.  This blocks the GetValues call until the
	// syncer has completed it's initial snapshot and is in sync.  The syncer is started
	// from the SetPrefixes() call from confd.
	c.waitForSync.Add(1)

	return c, nil
}

// client implements the StoreClient interface for confd, and also implements the
// Calico api.SyncerCallbacks and api.SyncerParseFailCallbacks interfaces for the
// BGP Syncer.
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
	watcherCond       *sync.Cond
}

// Called from confd to nofity this client of the full set of prefixes that will
// be watched.  We use this information to create the relevant syncer, based on whether
// we need to watch all nodes or just our own.
func (c *client) 	SetPrefixes(keys []string) error {
	log.Debug("Set prefixes called with: %v", keys)
	allNodes := false
	for _, k := range keys {
		// If we are watching the root bgp host node then we are watching all nodes.
		if k == allNodesKey {
			log.Debug("Watching all Calico nodes")
			allNodes = true
		}

		// Initialise the revision that we are watching for this prefix.  This will be updated
		// if we receive any syncer events for keys with this prefix.  The Watcher function will
		// then check the revisions it is interested in to see if there is an updated revision
		// that it needs to process.
		c.revisionsByPrefix[k] = 0
	}

	// Start the main syncer loop.
	nodeName := os.Getenv("NODENAME")
	c.syncer = bgpsyncer.New(c.client, c, nodeName, allNodes)
	c.syncer.Start()

	return nil
}

func (c *client) OnStatusUpdated(status api.SyncStatus) {
	// We should only get a single in-sync status update.  When we do, unblock the GetValues
	// calls.
	if status == api.InSync {
		c.cacheLock.Lock()
		defer c.cacheLock.Unlock()
		c.synced = true
		c.waitForSync.Done()
		log.Debug("Data is now syncd, can start rendering templates")
	}
}

func (c *client) OnUpdates(updates []api.Update) {
	// Update our cache from the updates.
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	// If we are in-sync then this is an incremental update, so increment our internal
	// cache revision.
	if c.synced {
		c.cacheRevision++
		log.Debug("Processing new updates, revision is now: %d", c.cacheRevision)
	}

	// Update our cache from each of the individual updates, and keep track of
	// any of the prefixes that are impacted.
	for _, u := range updates {
		// Update our cache of current entries.
		k, err := model.KeyToDefaultPath(u.Key)
		if err != nil {
			log.Error("Unable to create path from Key %v: %v", u.Key, err)
			continue
		}

		switch u.UpdateType {
		case api.UpdateTypeKVDeleted:
			delete(c.cache, k)
		case api.UpdateTypeKVNew, api.UpdateTypeKVUpdated:
			value, err := model.SerializeValue(&u.KVPair)
			if err != nil {
				log.Error("Unable to serialize value %v: %v", u.KVPair.Value, err)
				continue
			}
			c.cache[k] = string(value)
		}

		log.Debug("Cache entry updated from event type %d: %s=%s", u.UpdateType, k, c.cache[k])
		if c.synced {
			c.keyUpdated(k)
		}
	}

	// Wake up the watchers to let them know there may be some updates of interest.
	log.Debug("Notify watchers of new event data")
	c.watcherCond.Broadcast()
}

func (c *client) ParseFailed(rawKey string, rawValue string) {
	log.Error("Unable to parse datastore entry Key=%s; Value=%s", rawKey, rawValue)
}

// GetValues takes the etcd like keys and route it to the appropriate k8s API endpoint.
func (c *client) GetValues(keys []string) (map[string]string, error) {
	// We should block GetValues until we have the sync'd notification.  This is necessary because
	// our data collection is happening in a different goroutine.
	c.waitForSync.Wait()

	log.Debug("Requesting values for keys: %v", keys)

	// For simplicity always populate the results with the common set of default values
	// (event if we haven't been asked for them).
	values := map[string]string{}

	// The bird templates that confd is used to render assumes some global defaults are always
	// configured.  Add in these defaults if the required keys includes them.  The configured
	// values may override these.
	if c.matchesPrefix(globalLogging, keys) {
		values[globalLogging] = "info"
	}
	if c.matchesPrefix(globalASN, keys) {
		values[globalASN] = "64512"
	}
	if c.matchesPrefix(globalNodeMesh, keys) {
		values[globalNodeMesh] = `{"enabled": true}`
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

	log.Debug("Returning %d results", len(values))

	return values, nil
}

// WatchPrefix returns when the snapshot for any of the supplied prefix keys is changed from the
// last known waitIndex (revision).
//
// Since we keep track of revisions per prefix, all we need to do is check the revisions for an
// update, and if there is no update wait on the conditional which will be woken by the OnUpdates
// thread after updating the cache.
func (c *client) WatchPrefix(prefix string, keys []string, waitIndex uint64, stopChan chan bool) (uint64, error) {
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	if waitIndex == 0 {
		// If this is the first iteration, we always exit to ensure we render with the initial
		// synced settings.
		log.Debug("First watch call for template - exiting to render template")
		return c.cacheRevision, nil
	}

	for {
		// Loop through each key, if the revision associated with the key is higher than the waitIndex
		// then exit with the current cacheRevision and render with the current data.
		log.Debug("Checking for updated key revisions, watching from rev %d", waitIndex)
		for _, key := range keys {
			rev, ok := c.revisionsByPrefix[key]
			if !ok {
				log.Fatal("Watch prefix check for unknown prefix: ", key)
			}
			log.Debug("Found key prefix %s at rev %d", key, rev)
			if rev > waitIndex {
				log.Debug("Exiting to render template")
				return c.cacheRevision, nil
			}
		}

		// No changes for this watcher, so wait until there are more syncer events.
		log.Debug("No updated keys for this template - waiting for event notification")
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
		log.Debug("Prefix %s has rev %d", prefix, rev)
		if rev != c.cacheRevision && strings.HasPrefix(key, prefix) {
			log.Debug("Updating prefix to rev %d", c.cacheRevision)
			c.revisionsByPrefix[prefix] = c.cacheRevision
		}
	}
}
