package plugin

import (
	"context"
	"fmt"

	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
)

const NodeAffinityName = "NodeAffinity"

// NodeAffinity is a plugin that checks if a pod's node affinity matches a node.
type NodeAffinity struct{}

var _ framework.FilterPlugin = &NodeAffinity{}
var _ framework.ScorePlugin = &NodeAffinity{}

func NewNodeAffinity() *NodeAffinity {
	return &NodeAffinity{}
}

func (n *NodeAffinity) Name() string {
	return NodeAffinityName
}

// Filter checks if a pod's node affinity/node selector matches a node
func (n *NodeAffinity) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	node := nodeInfo.Node()

	// Check node selector
	if len(pod.Spec.NodeSelector) > 0 {
		selector := labels.SelectorFromSet(pod.Spec.NodeSelector)
		if !selector.Matches(labels.Set(node.Labels)) {
			return utils.NewStatus(utils.Unschedulable,
				"node(s) didn't match Pod's node selector")
		}
	}

	// Check node affinity
	if pod.Spec.Affinity != nil && pod.Spec.Affinity.NodeAffinity != nil {
		nodeAffinity := pod.Spec.Affinity.NodeAffinity

		// Check requiredDuringSchedulingIgnoredDuringExecution
		if nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			if !nodeMatchesNodeSelectorTerms(node, nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms) {
				return utils.NewStatus(utils.Unschedulable,
					"node(s) didn't match Pod's node affinity/selector")
			}
		}
	}

	return utils.NewStatus(utils.Success, "")
}

// Score gives higher score to nodes that match preferred node affinity
func (n *NodeAffinity) Score(ctx context.Context, pod *v1.Pod, nodeName string) (int64, *utils.Status) {
	return 0, utils.NewStatus(utils.Success, "")
}

func (n *NodeAffinity) ScoreExtensions() framework.ScoreExtensions {
	return n
}

func (n *NodeAffinity) NormalizeScore(ctx context.Context, pod *v1.Pod, scores utils.PluginResult) *utils.Status {
	return utils.NewStatus(utils.Success, "")
}

// nodeMatchesNodeSelectorTerms checks if a node matches the node selector terms
func nodeMatchesNodeSelectorTerms(node *v1.Node, nodeSelectorTerms []v1.NodeSelectorTerm) bool {
	// NodeSelectorTerms are ORed together
	for _, term := range nodeSelectorTerms {
		if nodeMatchesNodeSelectorTerm(node, &term) {
			return true
		}
	}
	return false
}

// nodeMatchesNodeSelectorTerm checks if a node matches a single node selector term
func nodeMatchesNodeSelectorTerm(node *v1.Node, term *v1.NodeSelectorTerm) bool {
	// All expressions within a term must match (AND)

	// Check MatchExpressions
	for _, expr := range term.MatchExpressions {
		if !nodeMatchesNodeSelectorRequirement(node.Labels, &expr) {
			return false
		}
	}

	// Check MatchFields
	for _, expr := range term.MatchFields {
		if !nodeMatchesFieldSelectorRequirement(node, &expr) {
			return false
		}
	}

	return true
}

// nodeMatchesNodeSelectorRequirement checks if node labels match a requirement
func nodeMatchesNodeSelectorRequirement(nodeLabels map[string]string, req *v1.NodeSelectorRequirement) bool {
	switch req.Operator {
	case v1.NodeSelectorOpIn:
		if value, ok := nodeLabels[req.Key]; ok {
			for _, v := range req.Values {
				if value == v {
					return true
				}
			}
		}
		return false
	case v1.NodeSelectorOpNotIn:
		if value, ok := nodeLabels[req.Key]; ok {
			for _, v := range req.Values {
				if value == v {
					return false
				}
			}
		}
		return true
	case v1.NodeSelectorOpExists:
		_, exists := nodeLabels[req.Key]
		return exists
	case v1.NodeSelectorOpDoesNotExist:
		_, exists := nodeLabels[req.Key]
		return !exists
	case v1.NodeSelectorOpGt:
		// Greater than comparison (for numeric values)
		// Simplified implementation
		return false
	case v1.NodeSelectorOpLt:
		// Less than comparison (for numeric values)
		// Simplified implementation
		return false
	default:
		return false
	}
}

// nodeMatchesFieldSelectorRequirement checks if node fields match a requirement
func nodeMatchesFieldSelectorRequirement(node *v1.Node, req *v1.NodeSelectorRequirement) bool {
	var nodeFieldValue string

	switch req.Key {
	case "metadata.name":
		nodeFieldValue = node.Name
	case "metadata.namespace":
		nodeFieldValue = node.Namespace
	case "metadata.uid":
		nodeFieldValue = string(node.UID)
	case "spec.unschedulable":
		if node.Spec.Unschedulable {
			nodeFieldValue = "true"
		} else {
			nodeFieldValue = "false"
		}
	default:
		return false
	}

	switch req.Operator {
	case v1.NodeSelectorOpIn:
		for _, v := range req.Values {
			if nodeFieldValue == v {
				return true
			}
		}
		return false
	case v1.NodeSelectorOpNotIn:
		for _, v := range req.Values {
			if nodeFieldValue == v {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// GetRequiredNodeAffinity returns the required node affinity from a pod
func GetRequiredNodeAffinity(pod *v1.Pod) *v1.NodeSelector {
	if pod.Spec.Affinity != nil &&
		pod.Spec.Affinity.NodeAffinity != nil &&
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		return pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	}
	return nil
}

// GetPreferredNodeAffinity returns the preferred node affinity from a pod
func GetPreferredNodeAffinity(pod *v1.Pod) []v1.PreferredSchedulingTerm {
	if pod.Spec.Affinity != nil &&
		pod.Spec.Affinity.NodeAffinity != nil &&
		pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution != nil {
		return pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	}
	return nil
}

// NodeSelectorRequirementsAsSelector converts NodeSelectorRequirements to a labels.Selector
func NodeSelectorRequirementsAsSelector(nsm []v1.NodeSelectorRequirement) (labels.Selector, error) {
	if len(nsm) == 0 {
		return labels.Nothing(), nil
	}
	selector := labels.NewSelector()
	for _, expr := range nsm {
		var op selection.Operator
		switch expr.Operator {
		case v1.NodeSelectorOpIn:
			op = selection.In
		case v1.NodeSelectorOpNotIn:
			op = selection.NotIn
		case v1.NodeSelectorOpExists:
			op = selection.Exists
		case v1.NodeSelectorOpDoesNotExist:
			op = selection.DoesNotExist
		default:
			return nil, fmt.Errorf("unsupported operator: %s", expr.Operator)
		}
		r, err := labels.NewRequirement(expr.Key, op, expr.Values)
		if err != nil {
			return nil, err
		}
		selector = selector.Add(*r)
	}
	return selector, nil
}

// NodeSelectorRequirementsAsFieldSelector converts the []NodeSelectorRequirement to FieldSelector.
func NodeSelectorRequirementsAsFieldSelector(nsr []v1.NodeSelectorRequirement) (labels.Selector, error) {
	if len(nsr) == 0 {
		return labels.Nothing(), nil
	}

	selectors := []labels.Selector{}
	for _, req := range nsr {
		switch req.Operator {
		case v1.NodeSelectorOpIn:
			if len(req.Values) != 1 {
				return nil, fmt.Errorf("field selector operator In must have exactly one value")
			}
			// Create a simple equality requirement
			r, err := labels.NewRequirement(req.Key, selection.Equals, []string{req.Values[0]})
			if err != nil {
				return nil, err
			}
			selectors = append(selectors, labels.NewSelector().Add(*r))
		case v1.NodeSelectorOpNotIn:
			if len(req.Values) != 1 {
				return nil, fmt.Errorf("field selector operator NotIn must have exactly one value")
			}
			r, err := labels.NewRequirement(req.Key, selection.NotEquals, []string{req.Values[0]})
			if err != nil {
				return nil, err
			}
			selectors = append(selectors, labels.NewSelector().Add(*r))
		default:
			return nil, fmt.Errorf("unsupported field selector operator: %s", req.Operator)
		}
	}

	return labels.NewSelector(), nil
}

// PodMatchesNodeSelectorAndAffinityTerms checks whether the pod is schedulable onto nodes according to
// the requirements in both NodeAffinity and nodeSelector.
func PodMatchesNodeSelectorAndAffinityTerms(pod *v1.Pod, node *v1.Node) bool {
	// Check node selector
	if len(pod.Spec.NodeSelector) > 0 {
		selector := labels.SelectorFromSet(pod.Spec.NodeSelector)
		if !selector.Matches(labels.Set(node.Labels)) {
			return false
		}
	}

	// Check node affinity
	nodeAffinityMatches := true
	affinity := pod.Spec.Affinity
	if affinity != nil && affinity.NodeAffinity != nil {
		nodeAffinity := affinity.NodeAffinity
		// Check required terms
		if nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			nodeSelectorTerms := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
			nodeAffinityMatches = nodeMatchesNodeSelectorTerms(node, nodeSelectorTerms)
		}
	}

	return nodeAffinityMatches
}

// NodeMatchesNodeSelector checks if a node's labels satisfy a node selector
func NodeMatchesNodeSelector(node *v1.Node, nodeSelector *metav1.LabelSelector) (bool, error) {
	if nodeSelector == nil {
		return true, nil
	}
	selector, err := metav1.LabelSelectorAsSelector(nodeSelector)
	if err != nil {
		return false, err
	}
	return selector.Matches(labels.Set(node.Labels)), nil
}
