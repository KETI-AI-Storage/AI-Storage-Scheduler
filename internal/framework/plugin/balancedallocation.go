package plugin

import (
	"context"
	"fmt"
	"math"

	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
)

const BalancedAllocationName = "BalancedAllocation"

// BalancedAllocation is a score plugin that favors nodes with balanced resource allocation.
// It calculates the variance of resource fractions (CPU, Memory, GPU) and penalizes imbalance.
// This helps prevent resource fragmentation where a node has plenty of one resource but not others.
type BalancedAllocation struct {
	cache *utils.Cache
}

var _ framework.ScorePlugin = &BalancedAllocation{}

func NewBalancedAllocation(cache *utils.Cache) *BalancedAllocation {
	return &BalancedAllocation{
		cache: cache,
	}
}

func (b *BalancedAllocation) Name() string {
	return BalancedAllocationName
}

// Score calculates the score based on resource balance
// Lower variance = higher score (more balanced)
func (b *BalancedAllocation) Score(ctx context.Context, pod *v1.Pod, nodeName string) (int64, *utils.Status) {
	nodeInfo := b.cache.Nodes()[nodeName]
	if nodeInfo == nil {
		return 0, utils.NewStatus(utils.Error, "node not found in cache")
	}

	score, err := b.scoreNode(pod, nodeInfo)
	if err != nil {
		return 0, utils.NewStatus(utils.Error, err.Error())
	}

	return score, utils.NewStatus(utils.Success, "")
}

func (b *BalancedAllocation) ScoreExtensions() framework.ScoreExtensions {
	return b
}

func (b *BalancedAllocation) NormalizeScore(ctx context.Context, pod *v1.Pod, scores utils.PluginResult) *utils.Status {
	return utils.NewStatus(utils.Success, "")
}

// scoreNode calculates the balanced allocation score for a node
func (b *BalancedAllocation) scoreNode(pod *v1.Pod, nodeInfo *utils.NodeInfo) (int64, error) {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return 0, fmt.Errorf("node not found")
	}

	node := nodeInfo.Node()
	allocatable := node.Status.Allocatable
	requested := nodeInfo.Requested
	podRequests := PodRequests(pod)

	// Calculate resource fractions (0.0 to 1.0)
	fractions := []float64{}

	// CPU fraction
	cpuAllocatable := allocatable.Cpu().MilliValue()
	if cpuAllocatable > 0 {
		cpuRequested := requested.MilliCPU
		cpuPodRequest := int64(0)
		cpuFraction := float64(cpuRequested+cpuPodRequest) / float64(cpuAllocatable)
		if cpuReq, exists := podRequests[v1.ResourceCPU]; exists {
			cpuPodRequest = cpuReq.MilliValue()
		}
		if cpuFraction > 1.0 {
			cpuFraction = 1.0
		}
		fractions = append(fractions, cpuFraction)
	}

	// Memory fraction
	memAllocatable := allocatable.Memory().Value()
	if memAllocatable > 0 {
		memRequested := requested.Memory
		memPodRequest := int64(0)
		memFraction := float64(memRequested+memPodRequest) / float64(memAllocatable)
		if memReq, exists := podRequests[v1.ResourceMemory]; exists {
			memPodRequest = memReq.Value()
		}
		if memFraction > 1.0 {
			memFraction = 1.0
		}
		fractions = append(fractions, memFraction)
	}

	// Extended resources (GPU, etc.)
	for resourceName, allocatableQuantity := range allocatable {
		if !isNativeResource(resourceName) {
			resourceAllocatable := allocatableQuantity.Value()
			if resourceAllocatable > 0 {
				resourceRequested := requested.ScalarResources[resourceName]
				resourcePodRequest := int64(0)
				if podReq, exists := podRequests[resourceName]; exists {
					resourcePodRequest = podReq.Value()
				}
				resourceFraction := float64(resourceRequested+resourcePodRequest) / float64(resourceAllocatable)
				if resourceFraction > 1.0 {
					resourceFraction = 1.0
				}
				fractions = append(fractions, resourceFraction)
			}
		}
	}

	// Not enough resources to calculate variance
	if len(fractions) < 2 {
		return 100, nil // Neutral score
	}

	// Calculate mean
	var sum float64
	for _, fraction := range fractions {
		sum += fraction
	}
	mean := sum / float64(len(fractions))

	// Calculate variance
	var variance float64
	for _, fraction := range fractions {
		variance += math.Pow(fraction-mean, 2)
	}
	variance = variance / float64(len(fractions))

	// Standard deviation
	stdDev := math.Sqrt(variance)

	// Score: Lower standard deviation = higher score (more balanced)
	// stdDev ranges from 0 (perfect balance) to ~0.5 (worst case)
	// Convert to 0-100 score where 0 stdDev = 100 score
	score := int64((1.0 - math.Min(stdDev*2, 1.0)) * 100)

	return score, nil
}
