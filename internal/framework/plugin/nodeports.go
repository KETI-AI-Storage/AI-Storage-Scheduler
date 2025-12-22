package plugin

import (
	"context"
	"fmt"

	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
)

const NodePortsName = "NodePorts"

// NodePorts is a filter plugin that checks if a node has free ports for the requested pod ports.
type NodePorts struct{}

var _ framework.FilterPlugin = &NodePorts{}

func NewNodePorts() *NodePorts {
	return &NodePorts{}
}

func (p *NodePorts) Name() string {
	return NodePortsName
}

// Filter checks if the node has free ports for all requested host ports.
func (p *NodePorts) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	// Get all requested host ports from the pod
	wantPorts := getContainerPorts(pod)
	if len(wantPorts) == 0 {
		return utils.NewStatus(utils.Success, "")
	}

	// Check each requested port against used ports on the node
	for _, port := range wantPorts {
		if nodeInfo.UsedPorts.CheckConflict(port.HostIP, string(port.Protocol), port.HostPort) {
			return utils.NewStatus(utils.Unschedulable,
				fmt.Sprintf("node(s) didn't have free ports for the requested pod ports: %s/%d",
					port.Protocol, port.HostPort))
		}
	}

	return utils.NewStatus(utils.Success, "")
}

// getContainerPorts returns all host ports requested by the pod's containers
func getContainerPorts(pod *v1.Pod) []v1.ContainerPort {
	var ports []v1.ContainerPort
	for _, container := range pod.Spec.Containers {
		for _, port := range container.Ports {
			if port.HostPort > 0 {
				ports = append(ports, port)
			}
		}
	}
	for _, container := range pod.Spec.InitContainers {
		for _, port := range container.Ports {
			if port.HostPort > 0 {
				ports = append(ports, port)
			}
		}
	}
	return ports
}
