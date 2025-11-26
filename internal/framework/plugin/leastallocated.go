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
type LeastAllocated struct{}

var _ framework.ScorePlugin = &LeastAllocated{}

func NewLeastAllocated() *LeastAllocated {
	return &LeastAllocated{}
}

func (l *LeastAllocated) Name() string {
	return LeastAllocatedName
}

func (l *LeastAllocated) Score(ctx context.Context, pod *v1.Pod, nodeName string) (int64, *utils.Status) {
	return 0, utils.NewStatus(utils.Success, "")
}

func (l *LeastAllocated) ScoreExtensions() framework.ScoreExtensions {
	return l
}

// NormalizeScore normalizes the scores for all nodes
func (l *LeastAllocated) NormalizeScore(ctx context.Context, pod *v1.Pod, scores utils.PluginResult) *utils.Status {
	// Calculate score based on available resources
	// Score = ((Allocatable - Requested) / Allocatable) * MaxScore
	// Higher score means more available resources

	return utils.NewStatus(utils.Success, "")
}

// scoreNode calculates the score for a specific node
func (l *LeastAllocated) scoreNode(pod *v1.Pod, nodeInfo *utils.NodeInfo) (int64, error) {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return 0, fmt.Errorf("node not found")
	}

	allocatable := nodeInfo.Node().Status.Allocatable
	requested := nodeInfo.Requested

	// Calculate CPU utilization percentage
	cpuAllocatable := allocatable.Cpu().MilliValue()
	cpuRequested := requested.MilliCPU
	cpuFree := float64(cpuAllocatable-cpuRequested) / float64(cpuAllocatable)

	// Calculate Memory utilization percentage
	memAllocatable := allocatable.Memory().Value()
	memRequested := requested.Memory
	memFree := float64(memAllocatable-memRequested) / float64(memAllocatable)

	// Average of CPU and Memory free percentage
	score := int64((cpuFree + memFree) / 2 * 100)

	return score, nil
}
