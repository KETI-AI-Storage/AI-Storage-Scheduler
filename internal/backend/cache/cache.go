package cache

import (
	"context"
	"keti/ai-storage-scheduler/internal/framework"
	"sync"
	"time"

	logger "keti/ai-storage-scheduler/internal/log"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// New returns a Cache implementation.
// It automatically starts a go routine that manages expiration of assumed pods.
// "ttl" is how long the assumed pod will get expired.
// "ctx" is the context that would close the background goroutine.
func New(ctx context.Context) *Cache {
	cache := newCache(ctx)
	return cache
}

// nodeInfoListItem holds a NodeInfo pointer and acts as an item in a doubly
// linked list. When a NodeInfo is updated, it goes to the head of the list.
// The items closer to the head are the most recently updated items.
type nodeInfoListItem struct {
	info *framework.NodeInfo
	next *nodeInfoListItem
	prev *nodeInfoListItem
}

type Cache struct {
	stop   <-chan struct{}
	ttl    time.Duration
	period time.Duration
	// This mutex guards all fields within this cache struct.
	mu sync.RWMutex
	// a set of assumed pod keys.
	// The key could further be used to get an entry in podStates.
	assumedPods sets.Set[string]
	// a map from pod key to podState.
	podStates map[string]*podState
	nodes     map[string]*nodeInfoListItem
	// headNode points to the most recently updated NodeInfo in "nodes". It is the
	// head of the linked list.
	headNode *nodeInfoListItem
	// A map from image name to its ImageStateSummary.
	imageStates map[string]*framework.ImageStateSummary
}

type podState struct {
	pod *v1.Pod
	// Used by assumedPod to determinate expiration.
	// If deadline is nil, assumedPod will never expire.
	deadline *time.Time
	// Used to block cache from expiring assumedPod if binding still runs
	bindingFinished bool
	logger          *logger.PodLogger
}

func newCache(ctx context.Context) *Cache {
	return &Cache{
		ttl:         0 * time.Second,
		period:      1 * time.Second,
		stop:        ctx.Done(),
		nodes:       make(map[string]*nodeInfoListItem),
		assumedPods: sets.New[string](),
		podStates:   make(map[string]*podState),
		imageStates: make(map[string]*framework.ImageStateSummary),
	}
}

func newNodeInfoListItem(ni *framework.NodeInfo) *nodeInfoListItem {
	return &nodeInfoListItem{
		info: ni,
	}
}

type Dump struct {
	AssumedPods sets.Set[string]
	Nodes       map[string]*framework.NodeInfo
}

func (cache *Cache) Dump() *Dump {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	nodes := make(map[string]*framework.NodeInfo, len(cache.nodes))
	for k, v := range cache.nodes {
		nodes[k] = v.info.Snapshot()
	}

	return &Dump{
		Nodes:       nodes,
		AssumedPods: cache.assumedPods.Union(nil),
	}
}
