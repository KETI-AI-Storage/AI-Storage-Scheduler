package plugin

import (
	"context"

	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
)

const NodeUnschedulableName = "NodeUnschedulable"

// NodeUnschedulable is a plugin that filters out nodes that have .spec.unschedulable set to true.
type NodeUnschedulable struct{}

var _ framework.FilterPlugin = &NodeUnschedulable{}

func NewNodeUnschedulable() *NodeUnschedulable {
	return &NodeUnschedulable{}
}

func (n *NodeUnschedulable) Name() string {
	return NodeUnschedulableName
}

// Filter checks if the node is unschedulable
func (n *NodeUnschedulable) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	node := nodeInfo.Node()

	// Check if node is marked as unschedulable
	// Note: DaemonSet pods are allowed on unschedulable nodes
	if node.Spec.Unschedulable && !isControlledByDaemonSet(pod) {
		return utils.NewStatus(utils.Unschedulable,
			"node(s) were unschedulable")
	}

	return utils.NewStatus(utils.Success, "")
}

// isControlledByDaemonSet checks if a pod is controlled by a DaemonSet
func isControlledByDaemonSet(pod *v1.Pod) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}
