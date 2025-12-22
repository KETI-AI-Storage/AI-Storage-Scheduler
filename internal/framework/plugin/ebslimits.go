package plugin

import (
	"context"
	"fmt"

	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
)

const EBSLimitsName = "EBSLimits"

// Default max number of EBS volumes per node
// This is based on AWS instance type limits (m5.large and similar)
const DefaultMaxEBSNitroVolumes = 25
const DefaultMaxEBSNonNitroVolumes = 39

// EBSLimits is a filter plugin that checks AWS EBS volume limits.
type EBSLimits struct {
	maxVolumesPerNode int
}

var _ framework.FilterPlugin = &EBSLimits{}

func NewEBSLimits() *EBSLimits {
	return &EBSLimits{
		maxVolumesPerNode: DefaultMaxEBSNonNitroVolumes,
	}
}

func NewEBSLimitsWithMax(maxVolumes int) *EBSLimits {
	return &EBSLimits{
		maxVolumesPerNode: maxVolumes,
	}
}

func (p *EBSLimits) Name() string {
	return EBSLimitsName
}

// Filter checks if adding the pod's EBS volumes would exceed the node's limit
func (p *EBSLimits) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	node := nodeInfo.Node()

	// Check if this is an AWS node
	if !isAWSNode(node) {
		return utils.NewStatus(utils.Success, "")
	}

	// Count existing EBS volumes on the node
	existingCount := countEBSVolumes(nodeInfo)

	// Count EBS volumes requested by the pod
	podCount := countPodEBSVolumes(pod)

	if podCount == 0 {
		return utils.NewStatus(utils.Success, "")
	}

	// Get limit from node allocatable or use default
	limit := p.getNodeLimit(node)

	if existingCount+podCount > limit {
		return utils.NewStatus(utils.Unschedulable,
			fmt.Sprintf("node %s has %d EBS volumes, adding %d would exceed limit of %d",
				node.Name, existingCount, podCount, limit))
	}

	return utils.NewStatus(utils.Success, "")
}

// isAWSNode checks if the node is an AWS node
func isAWSNode(node *v1.Node) bool {
	// Check provider ID
	if node.Spec.ProviderID != "" {
		if len(node.Spec.ProviderID) > 4 && node.Spec.ProviderID[:4] == "aws:" {
			return true
		}
	}

	// Check node labels
	if _, ok := node.Labels["node.kubernetes.io/instance-type"]; ok {
		if region, ok := node.Labels["topology.kubernetes.io/region"]; ok {
			// AWS regions start with specific prefixes
			if len(region) >= 2 {
				prefix := region[:2]
				if prefix == "us" || prefix == "eu" || prefix == "ap" || prefix == "sa" || prefix == "ca" || prefix == "me" || prefix == "af" {
					return true
				}
			}
		}
	}

	return false
}

// countEBSVolumes counts EBS volumes currently on the node
func countEBSVolumes(nodeInfo *utils.NodeInfo) int {
	count := 0
	for _, podInfo := range nodeInfo.Pods {
		count += countPodEBSVolumes(podInfo.Pod)
	}
	return count
}

// countPodEBSVolumes counts EBS volumes in a pod
func countPodEBSVolumes(pod *v1.Pod) int {
	count := 0
	for _, v := range pod.Spec.Volumes {
		if v.AWSElasticBlockStore != nil {
			count++
		}
		// Also count PVCs that might be EBS-backed
		// In a full implementation, we'd check the PV's source
	}
	return count
}

// getNodeLimit gets the EBS volume limit for a node
func (p *EBSLimits) getNodeLimit(node *v1.Node) int {
	// Check for attachable-volumes-aws-ebs in allocatable
	if quantity, ok := node.Status.Allocatable["attachable-volumes-aws-ebs"]; ok {
		return int(quantity.Value())
	}

	// Check instance type for Nitro-based instances (higher limits)
	if instanceType, ok := node.Labels["node.kubernetes.io/instance-type"]; ok {
		if isNitroInstance(instanceType) {
			return DefaultMaxEBSNitroVolumes
		}
	}

	return p.maxVolumesPerNode
}

// isNitroInstance checks if the instance type is Nitro-based
func isNitroInstance(instanceType string) bool {
	// Nitro instances: c5, m5, r5, t3, i3en, etc.
	nitroFamilies := []string{"c5", "c6", "m5", "m6", "r5", "r6", "t3", "i3en", "g4", "p4", "inf1", "dl1"}
	for _, family := range nitroFamilies {
		if len(instanceType) >= len(family) && instanceType[:len(family)] == family {
			return true
		}
	}
	return false
}
