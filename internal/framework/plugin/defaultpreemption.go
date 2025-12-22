package plugin

import (
	"context"
	"sort"

	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

const DefaultPreemptionName = "DefaultPreemption"

// DefaultPreemption is a PostFilter plugin that implements default preemption logic.
// When no nodes pass filtering, it tries to find a node where lower-priority pods
// can be evicted to make room for the pending pod.
type DefaultPreemption struct {
	k8sClient kubernetes.Interface
	cache     *utils.Cache
}

var _ framework.PostFilterPlugin = &DefaultPreemption{}

func NewDefaultPreemption(k8sClient kubernetes.Interface, cache *utils.Cache) *DefaultPreemption {
	return &DefaultPreemption{
		k8sClient: k8sClient,
		cache:     cache,
	}
}

func (p *DefaultPreemption) Name() string {
	return DefaultPreemptionName
}

// PostFilter tries to find a node for preemption
func (p *DefaultPreemption) PostFilter(ctx context.Context, pod *v1.Pod, filteredNodeStatusMap map[string]*utils.Status) (*framework.PostFilterResult, *utils.Status) {
	if p.cache == nil {
		return nil, utils.NewStatus(utils.Unschedulable, "cache not available for preemption")
	}

	// Get the pod's priority
	podPriority := getPodPriority(pod)

	// Find candidate nodes for preemption
	candidates := p.findPreemptionCandidates(ctx, pod, podPriority, filteredNodeStatusMap)
	if len(candidates) == 0 {
		return nil, utils.NewStatus(utils.Unschedulable, "no preemption candidates found")
	}

	// Sort candidates by number of pods to evict (prefer fewer evictions)
	sort.Slice(candidates, func(i, j int) bool {
		return len(candidates[i].victims) < len(candidates[j].victims)
	})

	// Return the best candidate
	best := candidates[0]
	return &framework.PostFilterResult{
		NominatedNodeName: best.nodeName,
	}, utils.NewStatus(utils.Success, "")
}

type preemptionCandidate struct {
	nodeName string
	victims  []*v1.Pod
}

// findPreemptionCandidates finds nodes where preemption could help
func (p *DefaultPreemption) findPreemptionCandidates(ctx context.Context, pod *v1.Pod, podPriority int32, filteredNodeStatusMap map[string]*utils.Status) []preemptionCandidate {
	var candidates []preemptionCandidate

	for nodeName, status := range filteredNodeStatusMap {
		// Only consider unschedulable status (not errors)
		if status.Code() != utils.Unschedulable {
			continue
		}

		nodeInfo := p.cache.Nodes()[nodeName]
		if nodeInfo == nil || nodeInfo.Node() == nil {
			continue
		}

		// Find pods with lower priority that could be evicted
		victims := p.findVictims(pod, podPriority, nodeInfo)
		if len(victims) == 0 {
			continue
		}

		// Check if evicting victims would make the node suitable
		if p.wouldFit(pod, nodeInfo, victims) {
			candidates = append(candidates, preemptionCandidate{
				nodeName: nodeName,
				victims:  victims,
			})
		}
	}

	return candidates
}

// findVictims finds pods with lower priority that could be evicted
func (p *DefaultPreemption) findVictims(pod *v1.Pod, podPriority int32, nodeInfo *utils.NodeInfo) []*v1.Pod {
	var victims []*v1.Pod

	for _, podInfo := range nodeInfo.Pods {
		existingPod := podInfo.Pod

		// Skip pods in system namespace
		if existingPod.Namespace == "kube-system" {
			continue
		}

		// Skip pods with higher or equal priority
		existingPriority := getPodPriority(existingPod)
		if existingPriority >= podPriority {
			continue
		}

		// Skip pods with PodDisruptionBudget protection (simplified check)
		// In a full implementation, this would check actual PDBs

		victims = append(victims, existingPod)
	}

	// Sort victims by priority (lowest first)
	sort.Slice(victims, func(i, j int) bool {
		return getPodPriority(victims[i]) < getPodPriority(victims[j])
	})

	return victims
}

// wouldFit checks if evicting victims would make the node suitable for the pod
func (p *DefaultPreemption) wouldFit(pod *v1.Pod, nodeInfo *utils.NodeInfo, victims []*v1.Pod) bool {
	// Calculate resources freed by evicting victims
	freedCPU := int64(0)
	freedMemory := int64(0)

	for _, victim := range victims {
		for _, container := range victim.Spec.Containers {
			if cpu, ok := container.Resources.Requests[v1.ResourceCPU]; ok {
				freedCPU += cpu.MilliValue()
			}
			if mem, ok := container.Resources.Requests[v1.ResourceMemory]; ok {
				freedMemory += mem.Value()
			}
		}
	}

	// Calculate resources needed by the new pod
	neededCPU := int64(0)
	neededMemory := int64(0)

	for _, container := range pod.Spec.Containers {
		if cpu, ok := container.Resources.Requests[v1.ResourceCPU]; ok {
			neededCPU += cpu.MilliValue()
		}
		if mem, ok := container.Resources.Requests[v1.ResourceMemory]; ok {
			neededMemory += mem.Value()
		}
	}

	// Get current available resources
	node := nodeInfo.Node()
	allocatable := node.Status.Allocatable
	requested := nodeInfo.Requested

	availableCPU := allocatable.Cpu().MilliValue() - requested.MilliCPU + freedCPU
	availableMemory := allocatable.Memory().Value() - requested.Memory + freedMemory

	return neededCPU <= availableCPU && neededMemory <= availableMemory
}

// getPodPriority returns the priority of a pod
func getPodPriority(pod *v1.Pod) int32 {
	if pod.Spec.Priority != nil {
		return *pod.Spec.Priority
	}
	return 0
}
