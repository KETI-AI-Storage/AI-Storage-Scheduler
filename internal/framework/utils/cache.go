package utils

import (
	"context"
	"fmt"
	"sync"
	"time"

	logger "keti/ai-storage-scheduler/internal/backend/log"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

type Cache struct {
	stop               <-chan struct{}
	mu                 sync.RWMutex
	nodes              map[string]*NodeInfo          // all node in cluster
	podStates          map[string]*podState          // all pod in cluster
	assumedPods        sets.Set[string]              // assumed that already scheduled
	imageStates        map[string]*ImageStateSummary // all images in cluster
	totalNodeCount     int
	availableNodeCount int
}

func NewCache(ctx context.Context) *Cache {
	return &Cache{
		stop:               ctx.Done(),
		nodes:              make(map[string]*NodeInfo),
		podStates:          make(map[string]*podState),
		imageStates:        make(map[string]*ImageStateSummary),
		totalNodeCount:     0,
		availableNodeCount: 0,
	}
}

func (cache *Cache) Nodes() map[string]*NodeInfo {
	if cache.nodes == nil {
		return nil
	}
	return cache.nodes
}

func (cache *Cache) InitCache(client *kubernetes.Clientset) error {
	nodes, err := client.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		logger.Info("Failed to init cache")
	}

	for _, node := range nodes.Items {
		cache.AddNode(&node, client)
	}

	return nil
}

func (cache *Cache) NodeInfoExist(pod *corev1.Pod) bool {
	if _, ok := cache.nodes[pod.Spec.NodeName]; ok {
		return true
	}
	return false
}

func (cache *Cache) AddNode(node *corev1.Node, client *kubernetes.Clientset) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	n, ok := cache.nodes[node.Name]
	if !ok {
		// podsInNode, _ := client.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{
		// 	FieldSelector: "specache.nodeName=" + node.Name,
		// })

		// podPointers := make([]*v1.Pod, len(podsInNode.Items))
		// for i := range podsInNode.Items {
		// 	podPointers[i] = &podsInNode.Items[i]
		// }

		n = NewNodeInfo( /*podPointers...*/ )
		cache.nodes[node.Name] = n
		cache.totalNodeCount++
	} else {
		cache.removeNodeImageStates(node)
		// cache.removePodStates(node)
	}

	cache.addNodeImageStates(node, n)
	n.SetNode(node)
	return nil
}

func (cache *Cache) UpdateNode(oldNode, newNode *corev1.Node) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	n, ok := cache.nodes[newNode.Name]
	if !ok {
		n = NewNodeInfo()
		cache.nodes[newNode.Name] = n
	} else {
		cache.removeNodeImageStates(n.Node())
	}

	cache.addNodeImageStates(newNode, n)
	n.SetNode(newNode)

	return nil
}

func (cache *Cache) RemoveNode(node *corev1.Node) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	n, ok := cache.nodes[node.Name]
	if !ok {
		return fmt.Errorf("node %v is not found", node.Name)
	}

	n.RemoveNode()
	cache.removeNodeImageStates(node)
	cache.removeNodeInfoFromList(node.Name)
	// cache.removePodStates(node)
	cache.totalNodeCount--

	return nil
}

func (cache *Cache) removeNodeInfoFromList(name string) {
	delete(cache.nodes, name)
}

func (cache *Cache) NodeCountDown() {
	cache.availableNodeCount--
}

func (cache *Cache) NodeCountUP() {
	cache.availableNodeCount++
}

func (cache *Cache) addNodeImageStates(node *corev1.Node, nodeInfo *NodeInfo) {
	newSum := make(map[string]*ImageStateSummary)

	for _, image := range node.Status.Images {
		for _, name := range image.Names {
			state, ok := cache.imageStates[name]
			if !ok {
				state = &ImageStateSummary{
					size:  image.SizeBytes,
					nodes: sets.New(node.Name),
				}
				cache.imageStates[name] = state
			} else {
				state.nodes.Insert(node.Name)
			}
			if _, ok := newSum[name]; !ok {
				newSum[name] = state
			}
		}
	}
	nodeInfo.ImageStates = newSum
}

func (cache *Cache) removeNodeImageStates(node *corev1.Node) {
	if node == nil {
		return
	}

	for _, image := range node.Status.Images {
		for _, name := range image.Names {
			state, ok := cache.imageStates[name]
			if ok {
				state.nodes.Delete(node.Name)
				if len(state.nodes) == 0 {
					delete(cache.imageStates, name)
				}
			}
		}
	}
}

func (cache *Cache) NodeCount() int {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return len(cache.nodes)
}

func (cache *Cache) PodCount() (int, error) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	count := 0
	for _, n := range cache.nodes {
		count += len(n.Pods)
	}
	return count, nil
}

func (cache *Cache) AddPod(pod *corev1.Pod) error {
	key, err := GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	currState, ok := cache.podStates[key]
	switch {
	case ok && cache.assumedPods.Has(key):
		if err = cache.updatePod(currState.pod, pod); err != nil {
			// logger.Error(err, "Error occurred while updating pod")
		}
		if currState.pod.Spec.NodeName != pod.Spec.NodeName {
			// logger.Info("Pod was added to a different node than it was assumed", "podKey", key, "pod", klog.KObj(pod), "assumedNode", klog.KRef("", pod.Specache.NodeName), "currentNode", klog.KRef("", currState.pod.Specache.NodeName))
			return nil
		}
	case !ok:
		if err = cache.addPod(pod, false); err != nil {
			// logger.Error(err, "Error occurred while adding pod")
		}
	default:
		return fmt.Errorf("pod %v(%v) was already in added state", key, klog.KObj(pod))
	}
	return nil
}

func (cache *Cache) addPod(pod *corev1.Pod, assumePod bool) error {
	key, err := GetPodKey(pod)
	if err != nil {
		return err
	}
	n, ok := cache.nodes[pod.Spec.NodeName]
	if !ok {
		n = NewNodeInfo()
		cache.nodes[pod.Spec.NodeName] = n
		cache.totalNodeCount++
	}
	n.AddPod(pod)
	ps := &podState{
		pod: pod,
	}
	cache.podStates[key] = ps
	if assumePod {
		cache.assumedPods.Insert(key)
	}
	return nil
}

func (cache *Cache) UpdatePod(oldPod, newPod *corev1.Pod) error {
	key, err := GetPodKey(oldPod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	currState, ok := cache.podStates[key]
	if !ok {
		return fmt.Errorf("pod %v(%v) is not added to scheduler cache, so cannot be updated", key, klog.KObj(oldPod))
	}

	if cache.assumedPods.Has(key) {
		return fmt.Errorf("assumed pod %v(%v) should not be updated", key, klog.KObj(oldPod))
	}

	if currState.pod.Spec.NodeName != newPod.Spec.NodeName {
		// logger.Error(nil, "Pod updated on a different node than previously added to", "podKey", key, "pod", klog.KObj(oldPod))
		// logger.Error(nil, "scheduler cache is corrupted and can badly affect scheduling decisions")
		// klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	return cache.updatePod(oldPod, newPod)
}

func (cache *Cache) updatePod(oldPod, newPod *corev1.Pod) error {
	if err := cache.removePod(oldPod); err != nil {
		return err
	}
	return cache.addPod(newPod, false)
}

func (cache *Cache) RemovePod(pod *corev1.Pod) error {
	key, err := GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	currState, ok := cache.podStates[key]

	if !ok {
		return fmt.Errorf("pod %v(%v) is not found in scheduler cache, so cannot be removed from it", key, klog.KObj(pod))
	}
	if currState.pod.Spec.NodeName != pod.Spec.NodeName {
		if pod.Spec.NodeName != "" {
			// logger.Error(nil, "scheduler cache is corrupted and can badly affect scheduling decisions")
			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}
	}

	return cache.removePod(currState.pod)
}

func (cache *Cache) removePod(pod *corev1.Pod) error {
	key, err := GetPodKey(pod)
	if err != nil {
		return err
	}

	n, ok := cache.nodes[pod.Spec.NodeName]
	if !ok {
		// logger.Error(nil, "Node not found when trying to remove pod", "node", klog.KRef("", pod.Spec.NodeName), "podKey", key, "pod", klog.KObj(pod))
	} else {
		if err := n.RemovePod(pod); err != nil {
			return err
		}
		if len(n.Pods) == 0 && n.Node() == nil {
			cache.removeNodeInfoFromList(pod.Spec.NodeName)
		}
	}

	delete(cache.podStates, key)
	delete(cache.assumedPods, key)
	return nil
}

func (cache *Cache) AssumePod(logger klog.Logger, pod *corev1.Pod) error {
	key, err := GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, ok := cache.podStates[key]; ok {
		return fmt.Errorf("pod %v(%v) is in the cache, so can't be assumed", key, klog.KObj(pod))
	}

	return cache.addPod(pod, true)
}

func (cache *Cache) IsAssumedPod(pod *corev1.Pod) (bool, error) {
	key, err := GetPodKey(pod)
	if err != nil {
		return false, err
	}

	cache.mu.RLock()
	defer cache.mu.RUnlock()

	return cache.assumedPods.Has(key), nil
}

func (cache *Cache) FinishBinding(pod *corev1.Pod) error {
	return cache.finishBinding(pod, time.Now())
}

func (cache *Cache) finishBinding(pod *corev1.Pod, now time.Time) error {
	key, err := GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.RLock()
	defer cache.mu.RUnlock()

	currState, ok := cache.podStates[key]
	if ok && cache.assumedPods.Has(key) {
		currState.bindingFinished = true
	}
	return nil
}

type ImageStateSummary struct {
	size     int64
	numNodes int
	nodes    sets.Set[string]
}

func NewImageStateSummary() *ImageStateSummary {
	return &ImageStateSummary{}
}
