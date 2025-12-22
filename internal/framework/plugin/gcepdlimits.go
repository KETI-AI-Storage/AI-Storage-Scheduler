package plugin

import (
	"context"
	"fmt"

	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
)

const GCEPDLimitsName = "GCEPDLimits"

// Default max number of GCE Persistent Disk volumes per node
// GCE limits: 16 PD per VM (can be increased to 128 with specific instance types)
const DefaultMaxGCEPDVolumes = 16
const MaxGCEPDVolumesWithLimit = 128

// GCEPDLimits is a filter plugin that checks GCE Persistent Disk volume limits.
type GCEPDLimits struct {
	maxVolumesPerNode int
}

var _ framework.FilterPlugin = &GCEPDLimits{}

func NewGCEPDLimits() *GCEPDLimits {
	return &GCEPDLimits{
		maxVolumesPerNode: DefaultMaxGCEPDVolumes,
	}
}

func NewGCEPDLimitsWithMax(maxVolumes int) *GCEPDLimits {
	return &GCEPDLimits{
		maxVolumesPerNode: maxVolumes,
	}
}

func (p *GCEPDLimits) Name() string {
	return GCEPDLimitsName
}

// Filter checks if adding the pod's GCE PD volumes would exceed the node's limit
func (p *GCEPDLimits) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	node := nodeInfo.Node()

	// Check if this is a GCE node
	if !isGCENode(node) {
		return utils.NewStatus(utils.Success, "")
	}

	// Count existing GCE PD volumes on the node
	existingCount := countGCEPDVolumes(nodeInfo)

	// Count GCE PD volumes requested by the pod
	podCount := countPodGCEPDVolumes(pod)

	if podCount == 0 {
		return utils.NewStatus(utils.Success, "")
	}

	// Get limit from node allocatable or use default
	limit := p.getNodeLimit(node)

	if existingCount+podCount > limit {
		return utils.NewStatus(utils.Unschedulable,
			fmt.Sprintf("node %s has %d GCE PD volumes, adding %d would exceed limit of %d",
				node.Name, existingCount, podCount, limit))
	}

	return utils.NewStatus(utils.Success, "")
}

// isGCENode checks if the node is a GCE node
func isGCENode(node *v1.Node) bool {
	// Check provider ID
	if node.Spec.ProviderID != "" {
		if len(node.Spec.ProviderID) > 4 && node.Spec.ProviderID[:4] == "gce:" {
			return true
		}
	}

	// Check node labels for GCE-specific labels
	if _, ok := node.Labels["cloud.google.com/gke-nodepool"]; ok {
		return true
	}

	// Check topology region for GCE regions
	if region, ok := node.Labels["topology.kubernetes.io/region"]; ok {
		gceRegionPrefixes := []string{"us-", "europe-", "asia-", "australia-", "northamerica-", "southamerica-"}
		for _, prefix := range gceRegionPrefixes {
			if len(region) >= len(prefix) && region[:len(prefix)] == prefix {
				return true
			}
		}
	}

	return false
}

// countGCEPDVolumes counts GCE PD volumes currently on the node
func countGCEPDVolumes(nodeInfo *utils.NodeInfo) int {
	count := 0
	for _, podInfo := range nodeInfo.Pods {
		count += countPodGCEPDVolumes(podInfo.Pod)
	}
	return count
}

// countPodGCEPDVolumes counts GCE PD volumes in a pod
func countPodGCEPDVolumes(pod *v1.Pod) int {
	count := 0
	for _, v := range pod.Spec.Volumes {
		if v.GCEPersistentDisk != nil {
			count++
		}
		// Also count PVCs that might be GCE PD-backed
		// In a full implementation, we'd check the PV's source
	}
	return count
}

// getNodeLimit gets the GCE PD volume limit for a node
func (p *GCEPDLimits) getNodeLimit(node *v1.Node) int {
	// Check for attachable-volumes-gce-pd in allocatable
	if quantity, ok := node.Status.Allocatable["attachable-volumes-gce-pd"]; ok {
		return int(quantity.Value())
	}

	// Check machine type for higher limits
	if machineType, ok := node.Labels["node.kubernetes.io/instance-type"]; ok {
		// n2, n2d, c2, m2 series support more disks
		highDiskFamilies := []string{"n2-", "n2d-", "c2-", "m2-", "a2-"}
		for _, family := range highDiskFamilies {
			if len(machineType) >= len(family) && machineType[:len(family)] == family {
				return MaxGCEPDVolumesWithLimit
			}
		}
	}

	return p.maxVolumesPerNode
}
