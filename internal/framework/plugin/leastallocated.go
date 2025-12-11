package plugin

import (
	"context"
	"fmt"

	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
)

const LeastAllocatedName = "LeastAllocated"

// LeastAllocated is a score plugin that favors nodes with fewer requested resources.
// This implements the "bin-packing avoidance" strategy - spreading workloads across nodes
// to prevent resource fragmentation and maintain capacity for large workloads.
type LeastAllocated struct {
	cache *utils.Cache
}

var _ framework.ScorePlugin = &LeastAllocated{}

func NewLeastAllocated(cache *utils.Cache) *LeastAllocated {
	return &LeastAllocated{
		cache: cache,
	}
}

func (l *LeastAllocated) Name() string {
	return LeastAllocatedName
}

// Score calculates the score for a node based on available resources
// Higher score = more free resources (less allocated)
func (l *LeastAllocated) Score(ctx context.Context, pod *v1.Pod, nodeName string) (int64, *utils.Status) {
	nodeInfo := l.cache.Nodes()[nodeName]
	if nodeInfo == nil {
		return 0, utils.NewStatus(utils.Error, "node not found in cache")
	}

	score, err := l.scoreNode(pod, nodeInfo)
	if err != nil {
		return 0, utils.NewStatus(utils.Error, err.Error())
	}

	return score, utils.NewStatus(utils.Success, "")
}

func (l *LeastAllocated) ScoreExtensions() framework.ScoreExtensions {
	return l
}

// NormalizeScore normalizes the scores for all nodes
func (l *LeastAllocated) NormalizeScore(ctx context.Context, pod *v1.Pod, scores utils.PluginResult) *utils.Status {
	// Scores are already normalized to 0-100 range in scoreNode
	return utils.NewStatus(utils.Success, "")
}

// scoreNode calculates the score for a specific node
// Score = ((Allocatable - Requested) / Allocatable) * 100
// Higher score means more available resources
func (l *LeastAllocated) scoreNode(pod *v1.Pod, nodeInfo *utils.NodeInfo) (int64, error) {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return 0, fmt.Errorf("node not found")
	}

	node := nodeInfo.Node()
	allocatable := node.Status.Allocatable
	requested := nodeInfo.Requested

	// Get pod requests to consider them in scoring
	podRequests := PodRequests(pod)

	// Calculate weighted resource scores
	totalWeight := int64(0)
	totalScore := int64(0)

	// CPU Score (weight: 1)
	cpuAllocatable := allocatable.Cpu().MilliValue()
	if cpuAllocatable > 0 {
		cpuRequested := requested.MilliCPU
		cpuPodRequest := podRequests[v1.ResourceCPU]

		// Available after scheduling this pod
		cpuAvailable := cpuAllocatable - cpuRequested - cpuPodRequest.MilliValue()
		if cpuAvailable < 0 {
			cpuAvailable = 0
		}

		cpuScore := (cpuAvailable * 100) / cpuAllocatable
		totalScore += cpuScore
		totalWeight += 1
	}

	// Memory Score (weight: 1)
	memAllocatable := allocatable.Memory().Value()
	if memAllocatable > 0 {
		memRequested := requested.Memory
		memPodRequest := podRequests[v1.ResourceMemory]

		// Available after scheduling this pod
		memAvailable := memAllocatable - memRequested - memPodRequest.Value()
		if memAvailable < 0 {
			memAvailable = 0
		}

		memScore := (memAvailable * 100) / memAllocatable
		totalScore += memScore
		totalWeight += 1
	}

	// Extended Resources Score (e.g., nvidia.com/gpu) - weight: 2 for GPU
	for resourceName, allocatableQuantity := range allocatable {
		if !isNativeResource(resourceName) {
			// This is an extended resource (like GPU)
			weight := int64(1)

			// Give higher weight to GPU resources
			if resourceName == "nvidia.com/gpu" {
				weight = 2
			}

			resourceAllocatable := allocatableQuantity.Value()
			if resourceAllocatable > 0 {
				resourceRequested := requested.ScalarResources[resourceName]
				resourcePodRequest := int64(0)
				if podReq, exists := podRequests[resourceName]; exists {
					resourcePodRequest = podReq.Value()
				}

				// Available after scheduling this pod
				resourceAvailable := resourceAllocatable - resourceRequested - resourcePodRequest
				if resourceAvailable < 0 {
					resourceAvailable = 0
				}

				resourceScore := (resourceAvailable * 100) / resourceAllocatable
				totalScore += resourceScore * weight
				totalWeight += weight
			}
		}
	}

	// Calculate final score as weighted average
	if totalWeight == 0 {
		return 0, nil
	}

	finalScore := totalScore / totalWeight
	return finalScore, nil
}
