package plugin

import (
	"context"

	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
)

const NodeNameName = "NodeName"

// NodeName is a plugin that checks if a pod's spec.nodeName matches the node.
type NodeName struct{}

var _ framework.FilterPlugin = &NodeName{}

func NewNodeName() *NodeName {
	return &NodeName{}
}

func (n *NodeName) Name() string {
	return NodeNameName
}

// Filter checks if the pod's nodeName matches the current node
func (n *NodeName) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	// If pod has no nodeName specified, it can be scheduled on any node
	if pod.Spec.NodeName == "" {
		return utils.NewStatus(utils.Success, "")
	}

	// If pod has nodeName specified, it must match the current node
	if pod.Spec.NodeName != nodeInfo.Node().Name {
		return utils.NewStatus(utils.Unschedulable,
			"node(s) didn't match the requested hostname")
	}

	return utils.NewStatus(utils.Success, "")
}
