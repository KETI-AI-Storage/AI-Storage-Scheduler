package queue

import (
	"sync"
	"time"

	heap "keti/ai-storage-scheduler/internal/backend/heap"
	framework "keti/ai-storage-scheduler/internal/framework"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/scheduler/util"
	"k8s.io/utils/clock"
)

const (
	// DefaultPodMaxInUnschedulablePodsDuration is the default value for the maximum
	// time a pod can stay in unschedulablePods. If a pod stays in unschedulablePods
	// for longer than this value, the pod will be moved from unschedulablePods to
	// backoffQ or activeQ. If this value is empty, the default value (5min)
	// will be used.
	DefaultPodMaxInUnschedulablePodsDuration time.Duration = 5 * time.Minute
	// DefaultPodInitialBackoffDuration is the default value for the initial backoff duration
	// for unschedulable pods. To change the default podInitialBackoffDurationSeconds used by the
	// scheduler, update the ComponentConfig value in defaults.go
	DefaultPodInitialBackoffDuration time.Duration = 1 * time.Second
	// DefaultPodMaxBackoffDuration is the default value for the max backoff duration
	// for unschedulable pods. To change the default podMaxBackoffDurationSeconds used by the
	// scheduler, update the ComponentConfig value in defaults.go
	DefaultPodMaxBackoffDuration time.Duration = 10 * time.Second

	// Scheduling queue names
	activeQ           = "Active"
	backoffQ          = "Backoff"
	unschedulablePods = "Unschedulable"
	preEnqueue        = "PreEnqueue"
)

// by the checking result.
type PreEnqueueCheck func(pod *v1.Pod) bool

type PriorityQueue struct {
	*nominator // 재 스케줄링 최우선 파드

	stop  chan struct{}
	clock clock.Clock

	// lock takes precedence and should be taken first,
	// before any other locks in the queue (activeQueue.lock or nominator.nLock).
	// Correct locking order is: lock > activeQueue.lock > nominator.nLock.
	lock sync.RWMutex

	// pod initial backoff duration.
	podInitialBackoffDuration time.Duration
	// pod maximum backoff duration.
	podMaxBackoffDuration time.Duration
	// the maximum time a pod can stay in the unschedulablePods.
	podMaxInUnschedulablePodsDuration time.Duration

	activeQ *activeQueue
	// podBackoffQ is a heap ordered by backoff expiry. Pods which have completed backoff
	// are popped from this heap before the scheduler looks at activeQ
	podBackoffQ *heap.Heap[*framework.QueuedPodInfo]
	// unschedulablePods holds pods that have been tried and determined unschedulable.
	unschedulablePods *UnschedulablePods
	// moveRequestCycle caches the sequence number of scheduling cycle when we
	// received a move request. Unschedulable pods in and before this scheduling
	// cycle will be put back to activeQueue if we were trying to schedule them
	// when we received move request.
	// TODO: this will be removed after SchedulingQueueHint goes to stable and the feature gate is removed.
	moveRequestCycle int64

	// preEnqueuePluginMap is keyed with profile name, valued with registered preEnqueue plugins.
	// preEnqueuePluginMap map[string][]framework.PreEnqueuePlugin
	// queueingHintMap is keyed with profile name, valued with registered queueing hint functions.
	// queueingHintMap QueueingHintMapPerProfile

	nsLister listersv1.NamespaceLister

	// metricsRecorder metrics.MetricAsyncRecorder

	// isSchedulingQueueHintEnabled indicates whether the feature gate for the scheduling queue is enabled.
	isSchedulingQueueHintEnabled bool
}

func NewPriorityQueue(lessFn framework.LessFunc, informerFactory informers.SharedInformerFactory) *PriorityQueue {
	pq := &PriorityQueue{
		stop:                              make(chan struct{}),
		podInitialBackoffDuration:         DefaultPodInitialBackoffDuration,
		podMaxBackoffDuration:             DefaultPodMaxBackoffDuration,
		podMaxInUnschedulablePodsDuration: DefaultPodMaxInUnschedulablePodsDuration,
		activeQ:                           newActiveQueue(heap.New[*framework.QueuedPodInfo](podInfoKeyFunc, heap.LessFunc[*framework.QueuedPodInfo](lessFn))),
		unschedulablePods:                 newUnschedulablePods(),
		// queueingHintMap:                   options.queueingHintMap,
		moveRequestCycle:             -1,
		isSchedulingQueueHintEnabled: true,
	}
	pq.podBackoffQ = heap.New(podInfoKeyFunc, pq.podsCompareBackoffCompleted)
	pq.nsLister = informerFactory.Core().V1().Namespaces().Lister()
	podLister := informerFactory.Core().V1().Pods().Lister()
	pq.nominator = newPodNominator(podLister)

	return pq
}

func (p *PriorityQueue) podsCompareBackoffCompleted(pInfo1, pInfo2 *framework.QueuedPodInfo) bool {
	bo1 := p.getBackoffTime(pInfo1)
	bo2 := p.getBackoffTime(pInfo2)
	return bo1.Before(bo2)
}

func (p *PriorityQueue) getBackoffTime(podInfo *framework.QueuedPodInfo) time.Time {
	duration := p.calculateBackoffDuration(podInfo)
	backoffTime := podInfo.Timestamp.Add(duration)
	return backoffTime
}

func (p *PriorityQueue) calculateBackoffDuration(podInfo *framework.QueuedPodInfo) time.Duration {
	if podInfo.Attempts == 0 {
		return 0
	}

	duration := p.podInitialBackoffDuration
	for i := 1; i < podInfo.Attempts; i++ {
		if duration > p.podMaxBackoffDuration-duration {
			return p.podMaxBackoffDuration
		}
		duration += duration
	}
	return duration
}

type QueueingHintFunction struct {
	PluginName     string
	QueueingHintFn framework.QueueingHintFn
}

type clusterEvent struct {
	event  framework.ClusterEvent
	oldObj interface{}
	newObj interface{}
}

type activeQueue struct {
	lock   sync.RWMutex
	queue  *heap.Heap[*framework.QueuedPodInfo] // schedule. Head of heap is the highest priority pod.
	cond   sync.Cond
	closed bool
	// metricsRecorder metrics.MetricAsyncRecorder
}

func newActiveQueue(queue *heap.Heap[*framework.QueuedPodInfo]) *activeQueue {
	aq := &activeQueue{
		queue: queue,
	}
	aq.cond.L = &aq.lock

	return aq
}

// UnschedulablePods holds pods that cannot be scheduled. This data structure
// is used to implement unschedulablePods.
type UnschedulablePods struct {
	// podInfoMap is a map key by a pod's full-name and the value is a pointer to the QueuedPodInfo.
	podInfoMap map[string]*framework.QueuedPodInfo
	keyFunc    func(*v1.Pod) string
	// unschedulableRecorder, gatedRecorder metrics.MetricRecorder
}

// addOrUpdate adds a pod to the unschedulable podInfoMap.
func (u *UnschedulablePods) addOrUpdate(pInfo *framework.QueuedPodInfo) {
	podID := u.keyFunc(pInfo.Pod)
	if _, exists := u.podInfoMap[podID]; !exists {
		// if pInfo.Gated && u.gatedRecorder != nil {
		// 	u.gatedRecorder.Inc()
		// } else if !pInfo.Gated && u.unschedulableRecorder != nil {
		// 	u.unschedulableRecorder.Inc()
		// }
	}
	u.podInfoMap[podID] = pInfo
}

// delete deletes a pod from the unschedulable podInfoMap.
// The `gated` parameter is used to figure out which metric should be decreased.
func (u *UnschedulablePods) delete(pod *v1.Pod, gated bool) {
	podID := u.keyFunc(pod)
	if _, exists := u.podInfoMap[podID]; exists {
		// if gated && u.gatedRecorder != nil {
		// 	u.gatedRecorder.Dec()
		// } else if !gated && u.unschedulableRecorder != nil {
		// 	u.unschedulableRecorder.Dec()
		// }
	}
	delete(u.podInfoMap, podID)
}

// get returns the QueuedPodInfo if a pod with the same key as the key of the given "pod"
// is found in the map. It returns nil otherwise.
func (u *UnschedulablePods) get(pod *v1.Pod) *framework.QueuedPodInfo {
	podKey := u.keyFunc(pod)
	if pInfo, exists := u.podInfoMap[podKey]; exists {
		return pInfo
	}
	return nil
}

// clear removes all the entries from the unschedulable podInfoMap.
func (u *UnschedulablePods) clear() {
	u.podInfoMap = make(map[string]*framework.QueuedPodInfo)
	// if u.unschedulableRecorder != nil {
	// 	u.unschedulableRecorder.Clear()
	// }
	// if u.gatedRecorder != nil {
	// 	u.gatedRecorder.Clear()
	// }
}

// newUnschedulablePods initializes a new object of UnschedulablePods.
func newUnschedulablePods( /*unschedulableRecorder , gatedRecorder metrics.MetricRecorder*/ ) *UnschedulablePods {
	return &UnschedulablePods{
		podInfoMap: make(map[string]*framework.QueuedPodInfo),
		keyFunc:    util.GetPodFullName,
		// unschedulableRecorder: unschedulableRecorder,
		// gatedRecorder:         gatedRecorder,
	}
}

func podInfoKeyFunc(pInfo *framework.QueuedPodInfo) string {
	return cache.NewObjectName(pInfo.Pod.Namespace, pInfo.Pod.Name).String()
}
