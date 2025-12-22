package plugin

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
)

const AzureDiskLimitsName = "AzureDiskLimits"

// Default max number of Azure Disk volumes per node
// Azure limits vary by VM size: Standard_D2_v2 = 8, Standard_D4_v2 = 16, etc.
const DefaultMaxAzureDiskVolumes = 16

// AzureDiskLimits is a filter plugin that checks Azure Disk volume limits.
type AzureDiskLimits struct {
	maxVolumesPerNode int
}

var _ framework.FilterPlugin = &AzureDiskLimits{}

func NewAzureDiskLimits() *AzureDiskLimits {
	return &AzureDiskLimits{
		maxVolumesPerNode: DefaultMaxAzureDiskVolumes,
	}
}

func NewAzureDiskLimitsWithMax(maxVolumes int) *AzureDiskLimits {
	return &AzureDiskLimits{
		maxVolumesPerNode: maxVolumes,
	}
}

func (p *AzureDiskLimits) Name() string {
	return AzureDiskLimitsName
}

// Filter checks if adding the pod's Azure Disk volumes would exceed the node's limit
func (p *AzureDiskLimits) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	node := nodeInfo.Node()

	// Check if this is an Azure node
	if !isAzureNode(node) {
		return utils.NewStatus(utils.Success, "")
	}

	// Count existing Azure Disk volumes on the node
	existingCount := countAzureDiskVolumes(nodeInfo)

	// Count Azure Disk volumes requested by the pod
	podCount := countPodAzureDiskVolumes(pod)

	if podCount == 0 {
		return utils.NewStatus(utils.Success, "")
	}

	// Get limit from node allocatable or use default
	limit := p.getNodeLimit(node)

	if existingCount+podCount > limit {
		return utils.NewStatus(utils.Unschedulable,
			fmt.Sprintf("node %s has %d Azure Disk volumes, adding %d would exceed limit of %d",
				node.Name, existingCount, podCount, limit))
	}

	return utils.NewStatus(utils.Success, "")
}

// isAzureNode checks if the node is an Azure node
func isAzureNode(node *v1.Node) bool {
	// Check provider ID
	if node.Spec.ProviderID != "" {
		if len(node.Spec.ProviderID) > 6 && node.Spec.ProviderID[:6] == "azure:" {
			return true
		}
	}

	// Check node labels for Azure-specific labels
	if _, ok := node.Labels["kubernetes.azure.com/cluster"]; ok {
		return true
	}
	if _, ok := node.Labels["agentpool"]; ok {
		// AKS-specific label
		if _, ok2 := node.Labels["kubernetes.io/role"]; ok2 {
			return true
		}
	}

	// Check topology region for Azure regions
	if region, ok := node.Labels["topology.kubernetes.io/region"]; ok {
		azureRegions := []string{"eastus", "westus", "centralus", "northeurope", "westeurope",
			"southeastasia", "eastasia", "japaneast", "australiaeast", "brazilsouth"}
		for _, azRegion := range azureRegions {
			if region == azRegion {
				return true
			}
		}
	}

	return false
}

// countAzureDiskVolumes counts Azure Disk volumes currently on the node
func countAzureDiskVolumes(nodeInfo *utils.NodeInfo) int {
	count := 0
	for _, podInfo := range nodeInfo.Pods {
		count += countPodAzureDiskVolumes(podInfo.Pod)
	}
	return count
}

// countPodAzureDiskVolumes counts Azure Disk volumes in a pod
func countPodAzureDiskVolumes(pod *v1.Pod) int {
	count := 0
	for _, v := range pod.Spec.Volumes {
		if v.AzureDisk != nil {
			count++
		}
		// Also count PVCs that might be Azure Disk-backed
		// In a full implementation, we'd check the PV's source
	}
	return count
}

// getNodeLimit gets the Azure Disk volume limit for a node
func (p *AzureDiskLimits) getNodeLimit(node *v1.Node) int {
	// Check for attachable-volumes-azure-disk in allocatable
	if quantity, ok := node.Status.Allocatable["attachable-volumes-azure-disk"]; ok {
		return int(quantity.Value())
	}

	// Determine limit based on VM size
	if vmSize, ok := node.Labels["node.kubernetes.io/instance-type"]; ok {
		return getAzureVMDiskLimit(vmSize)
	}

	return p.maxVolumesPerNode
}

// getAzureVMDiskLimit returns the disk limit for an Azure VM size
func getAzureVMDiskLimit(vmSize string) int {
	vmSize = strings.ToLower(vmSize)

	// Parse the VM size to determine disk limits
	// Format: Standard_D{n}_v{version} or Standard_D{n}s_v{version}
	// The number typically corresponds to data disks: D2 = 8, D4 = 16, D8 = 32, etc.

	// D-series v2
	if strings.Contains(vmSize, "standard_d") {
		// Extract the number after 'd'
		parts := strings.Split(vmSize, "_")
		for _, part := range parts {
			if len(part) > 1 && part[0] == 'd' {
				numStr := ""
				for i := 1; i < len(part); i++ {
					if part[i] >= '0' && part[i] <= '9' {
						numStr += string(part[i])
					} else {
						break
					}
				}
				if num, err := strconv.Atoi(numStr); err == nil {
					// D2 = 8, D4 = 16, D8 = 32, D16 = 64, D32 = 64, D64 = 64
					switch {
					case num <= 2:
						return 8
					case num <= 4:
						return 16
					case num <= 8:
						return 32
					default:
						return 64
					}
				}
			}
		}
	}

	// E-series (memory optimized)
	if strings.Contains(vmSize, "standard_e") {
		return 32
	}

	// L-series (storage optimized)
	if strings.Contains(vmSize, "standard_l") {
		return 64
	}

	// M-series (memory and compute intensive)
	if strings.Contains(vmSize, "standard_m") {
		return 64
	}

	// N-series (GPU)
	if strings.Contains(vmSize, "standard_n") {
		return 24
	}

	return DefaultMaxAzureDiskVolumes
}
