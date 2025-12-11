package plugin

import (
	"context"
	"fmt"

	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
)

const NodeResourcesFitName = "NodeResourcesFit"

// NodeResourcesFit is a filter plugin that checks if a node has sufficient resources.
type NodeResourcesFit struct{}

var _ framework.FilterPlugin = &NodeResourcesFit{}

func NewNodeResourcesFit() *NodeResourcesFit {
	return &NodeResourcesFit{}
}

func (n *NodeResourcesFit) Name() string {
	return NodeResourcesFitName
}

func (n *NodeResourcesFit) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	// Calculate pod's resource requests
	podRequests := computePodResourceRequest(pod)

	// Get node's allocatable resources
	allocatable := nodeInfo.Node().Status.Allocatable

	// Get already allocated resources
	allocated := nodeInfo.Requested

	// Check CPU
	availableCPU := allocatable.Cpu().MilliValue() - allocated.MilliCPU
	if podRequests.MilliCPU > availableCPU {
		return utils.NewStatus(utils.Unschedulable,
			fmt.Sprintf("Insufficient cpu: requested %dm, available %dm",
				podRequests.MilliCPU, availableCPU))
	}

	// Check Memory
	availableMemory := allocatable.Memory().Value() - allocated.Memory
	if podRequests.Memory > availableMemory {
		return utils.NewStatus(utils.Unschedulable,
			fmt.Sprintf("Insufficient memory: requested %d, available %d",
				podRequests.Memory, availableMemory))
	}

	// Check Extended Resources (GPU, etc.)
	podRequestsList := PodRequests(pod)
	for resourceName, requestedQuantity := range podRequestsList {
		// Check extended resources like nvidia.com/gpu
		if !isNativeResource(resourceName) {
			allocatableQuantity, hasResource := allocatable[resourceName]
			if !hasResource {
				// Node doesn't have this extended resource at all
				if requestedQuantity.Value() > 0 {
					return utils.NewStatus(utils.Unschedulable,
						fmt.Sprintf("Insufficient %s: node doesn't have this resource", resourceName))
				}
				continue
			}

			// Get already allocated amount of this extended resource
			allocatedQuantity := int64(0)
			if allocated.ScalarResources != nil {
				allocatedQuantity = allocated.ScalarResources[resourceName]
			}
			availableQuantity := allocatableQuantity.Value() - allocatedQuantity

			if requestedQuantity.Value() > availableQuantity {
				return utils.NewStatus(utils.Unschedulable,
					fmt.Sprintf("Insufficient %s: requested %d, available %d",
						resourceName, requestedQuantity.Value(), availableQuantity))
			}
		}
	}

	return utils.NewStatus(utils.Success, "")
}

func computePodResourceRequest(pod *v1.Pod) *utils.Resource {
	result := &utils.Resource{}
	for _, container := range pod.Spec.Containers {
		result.Add(container.Resources.Requests)
	}

	// Take max of init containers
	for _, container := range pod.Spec.InitContainers {
		requests := container.Resources.Requests
		if cpu := requests[v1.ResourceCPU]; cpu.MilliValue() > result.MilliCPU {
			result.MilliCPU = cpu.MilliValue()
		}
		if memory := requests[v1.ResourceMemory]; memory.Value() > result.Memory {
			result.Memory = memory.Value()
		}
	}

	// If Overhead is defined, add it to the total requests
	if pod.Spec.Overhead != nil {
		result.Add(pod.Spec.Overhead)
	}

	return result
}

// PodRequests helper function
func PodRequests(pod *v1.Pod) v1.ResourceList {
	result := v1.ResourceList{}
	for _, container := range pod.Spec.Containers {
		addResourceList(result, container.Resources.Requests)
	}

	// Take max of init containers
	for _, container := range pod.Spec.InitContainers {
		for rName, rQuantity := range container.Resources.Requests {
			if existing, ok := result[rName]; !ok || rQuantity.Cmp(existing) > 0 {
				if result == nil {
					result = v1.ResourceList{}
				}
				result[rName] = rQuantity.DeepCopy()
			}
		}
	}

	// Add overhead
	if pod.Spec.Overhead != nil {
		addResourceList(result, pod.Spec.Overhead)
	}

	return result
}

func addResourceList(list v1.ResourceList, new v1.ResourceList) {
	for name, quantity := range new {
		if value, ok := list[name]; !ok {
			list[name] = quantity.DeepCopy()
		} else {
			value.Add(quantity)
			list[name] = value
		}
	}
}

// isNativeResource checks if a resource is a native Kubernetes resource (CPU, Memory, Storage, etc.)
func isNativeResource(resourceName v1.ResourceName) bool {
	switch resourceName {
	case v1.ResourceCPU, v1.ResourceMemory, v1.ResourceStorage, v1.ResourceEphemeralStorage,
		v1.ResourcePods, v1.ResourceHugePagesPrefix:
		return true
	default:
		// Check if it's a hugepages resource
		if string(resourceName)[:len(v1.ResourceHugePagesPrefix)] == string(v1.ResourceHugePagesPrefix) {
			return true
		}
		return false
	}
}
