package plugin

import (
	"context"
	"fmt"

	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const NodeVolumeLimitsName = "NodeVolumeLimits"

// DefaultMaxCSIVolumes is the default max number of CSI volumes per node
const DefaultMaxCSIVolumes = 256

// NodeVolumeLimits is a filter plugin that checks CSI volume limits.
// This plugin focuses on CSI driver-specific volume limits from CSINode.
type NodeVolumeLimits struct {
	k8sClient kubernetes.Interface
}

var _ framework.FilterPlugin = &NodeVolumeLimits{}

func NewNodeVolumeLimits(k8sClient kubernetes.Interface) *NodeVolumeLimits {
	return &NodeVolumeLimits{
		k8sClient: k8sClient,
	}
}

func (p *NodeVolumeLimits) Name() string {
	return NodeVolumeLimitsName
}

// Filter checks if adding the pod's CSI volumes would exceed the node's CSI driver limits
func (p *NodeVolumeLimits) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	node := nodeInfo.Node()

	// Count CSI volumes per driver in existing pods
	existingCSI := countCSIVolumesByDriver(nodeInfo)

	// Count CSI volumes per driver in the new pod
	podCSI := countPodCSIVolumesByDriver(pod)

	if len(podCSI) == 0 {
		return utils.NewStatus(utils.Success, "")
	}

	// Get CSI driver limits from CSINode
	driverLimits := p.getCSIDriverLimits(ctx, node.Name)

	// Check each driver
	for driver, podCount := range podCSI {
		existing := existingCSI[driver]
		total := existing + podCount

		// Get limit for this driver
		limit, hasLimit := driverLimits[driver]
		if !hasLimit {
			limit = DefaultMaxCSIVolumes
		}

		if total > limit {
			return utils.NewStatus(utils.Unschedulable,
				fmt.Sprintf("CSI driver %s: node has %d volumes, adding %d would exceed limit of %d",
					driver, existing, podCount, limit))
		}
	}

	return utils.NewStatus(utils.Success, "")
}

// countCSIVolumesByDriver counts CSI volumes per driver on the node
func countCSIVolumesByDriver(nodeInfo *utils.NodeInfo) map[string]int {
	counts := make(map[string]int)
	for _, podInfo := range nodeInfo.Pods {
		for driver, count := range countPodCSIVolumesByDriver(podInfo.Pod) {
			counts[driver] += count
		}
	}
	return counts
}

// countPodCSIVolumesByDriver counts CSI volumes per driver in a pod
func countPodCSIVolumesByDriver(pod *v1.Pod) map[string]int {
	counts := make(map[string]int)
	for _, v := range pod.Spec.Volumes {
		if v.CSI != nil {
			counts[v.CSI.Driver]++
		}
	}
	return counts
}

// getCSIDriverLimits gets CSI driver volume limits from CSINode
func (p *NodeVolumeLimits) getCSIDriverLimits(ctx context.Context, nodeName string) map[string]int {
	limits := make(map[string]int)

	if p.k8sClient == nil {
		return limits
	}

	// Get CSINode for this node
	csiNode, err := p.k8sClient.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		// CSINode not found, return empty limits (will use defaults)
		return limits
	}

	// Extract limits from each driver
	for _, driver := range csiNode.Spec.Drivers {
		if driver.Allocatable != nil && driver.Allocatable.Count != nil {
			limits[driver.Name] = int(*driver.Allocatable.Count)
		}
	}

	return limits
}
