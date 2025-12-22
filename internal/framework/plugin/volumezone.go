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

const VolumeZoneName = "VolumeZone"

// Common topology labels
const (
	LabelTopologyZone   = "topology.kubernetes.io/zone"
	LabelTopologyRegion = "topology.kubernetes.io/region"
	// Legacy labels for backward compatibility
	LabelZoneFailureDomain   = "failure-domain.beta.kubernetes.io/zone"
	LabelRegionFailureDomain = "failure-domain.beta.kubernetes.io/region"
)

// VolumeZone is a filter plugin that checks if volumes requested by the pod
// are available in the topology zone of the node.
type VolumeZone struct {
	k8sClient kubernetes.Interface
}

var _ framework.FilterPlugin = &VolumeZone{}

func NewVolumeZone(k8sClient kubernetes.Interface) *VolumeZone {
	return &VolumeZone{
		k8sClient: k8sClient,
	}
}

func (p *VolumeZone) Name() string {
	return VolumeZoneName
}

// Filter checks if the node is in the same zone as the volumes requested by the pod
func (p *VolumeZone) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	node := nodeInfo.Node()

	// Get node's zone and region
	nodeZone := getNodeZone(node)
	nodeRegion := getNodeRegion(node)

	// Check each volume
	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim == nil {
			continue
		}

		// Get the PVC
		pvc, err := p.getPVC(ctx, pod.Namespace, v.PersistentVolumeClaim.ClaimName)
		if err != nil {
			// If we can't get the PVC, skip the check
			continue
		}

		// If PVC is not bound yet, skip (VolumeBinding will handle this)
		if pvc.Status.Phase != v1.ClaimBound || pvc.Spec.VolumeName == "" {
			continue
		}

		// Get the PV
		pv, err := p.getPV(ctx, pvc.Spec.VolumeName)
		if err != nil {
			continue
		}

		// Check zone constraints from PV labels
		pvZone := getPVZone(pv)
		pvRegion := getPVRegion(pv)

		if pvZone != "" && nodeZone != "" && pvZone != nodeZone {
			return utils.NewStatus(utils.Unschedulable,
				fmt.Sprintf("volume %s is in zone %s, but node is in zone %s",
					pv.Name, pvZone, nodeZone))
		}

		if pvRegion != "" && nodeRegion != "" && pvRegion != nodeRegion {
			return utils.NewStatus(utils.Unschedulable,
				fmt.Sprintf("volume %s is in region %s, but node is in region %s",
					pv.Name, pvRegion, nodeRegion))
		}

		// Check node affinity constraints on PV
		if pv.Spec.NodeAffinity != nil && pv.Spec.NodeAffinity.Required != nil {
			if !nodeMatchesNodeAffinity(node, pv.Spec.NodeAffinity.Required) {
				return utils.NewStatus(utils.Unschedulable,
					fmt.Sprintf("volume %s has node affinity that doesn't match node %s",
						pv.Name, node.Name))
			}
		}
	}

	return utils.NewStatus(utils.Success, "")
}

func (p *VolumeZone) getPVC(ctx context.Context, namespace, name string) (*v1.PersistentVolumeClaim, error) {
	if p.k8sClient == nil {
		return nil, fmt.Errorf("kubernetes client not available")
	}
	return p.k8sClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
}

func (p *VolumeZone) getPV(ctx context.Context, name string) (*v1.PersistentVolume, error) {
	if p.k8sClient == nil {
		return nil, fmt.Errorf("kubernetes client not available")
	}
	return p.k8sClient.CoreV1().PersistentVolumes().Get(ctx, name, metav1.GetOptions{})
}

// getNodeZone returns the zone of the node
func getNodeZone(node *v1.Node) string {
	if zone, ok := node.Labels[LabelTopologyZone]; ok {
		return zone
	}
	if zone, ok := node.Labels[LabelZoneFailureDomain]; ok {
		return zone
	}
	return ""
}

// getNodeRegion returns the region of the node
func getNodeRegion(node *v1.Node) string {
	if region, ok := node.Labels[LabelTopologyRegion]; ok {
		return region
	}
	if region, ok := node.Labels[LabelRegionFailureDomain]; ok {
		return region
	}
	return ""
}

// getPVZone returns the zone of the PV
func getPVZone(pv *v1.PersistentVolume) string {
	if pv.Labels == nil {
		return ""
	}
	if zone, ok := pv.Labels[LabelTopologyZone]; ok {
		return zone
	}
	if zone, ok := pv.Labels[LabelZoneFailureDomain]; ok {
		return zone
	}
	return ""
}

// getPVRegion returns the region of the PV
func getPVRegion(pv *v1.PersistentVolume) string {
	if pv.Labels == nil {
		return ""
	}
	if region, ok := pv.Labels[LabelTopologyRegion]; ok {
		return region
	}
	if region, ok := pv.Labels[LabelRegionFailureDomain]; ok {
		return region
	}
	return ""
}

// nodeMatchesNodeAffinity checks if a node matches PV's node affinity
func nodeMatchesNodeAffinity(node *v1.Node, nodeSelector *v1.NodeSelector) bool {
	if nodeSelector == nil {
		return true
	}

	for _, term := range nodeSelector.NodeSelectorTerms {
		if nodeSelectorTermMatches(node, &term) {
			return true
		}
	}
	return false
}

// nodeSelectorTermMatches checks if a node matches a NodeSelectorTerm
func nodeSelectorTermMatches(node *v1.Node, term *v1.NodeSelectorTerm) bool {
	// Check match expressions
	for _, req := range term.MatchExpressions {
		if !nodeSelectorRequirementMatches(node.Labels, &req) {
			return false
		}
	}

	// Check match fields (for node-specific fields)
	for _, req := range term.MatchFields {
		var value string
		switch req.Key {
		case "metadata.name":
			value = node.Name
		default:
			continue
		}

		if !matchValue(value, &req) {
			return false
		}
	}

	return true
}

// nodeSelectorRequirementMatches checks if labels match a NodeSelectorRequirement
func nodeSelectorRequirementMatches(labels map[string]string, req *v1.NodeSelectorRequirement) bool {
	value, exists := labels[req.Key]

	switch req.Operator {
	case v1.NodeSelectorOpIn:
		if !exists {
			return false
		}
		for _, v := range req.Values {
			if v == value {
				return true
			}
		}
		return false

	case v1.NodeSelectorOpNotIn:
		if !exists {
			return true
		}
		for _, v := range req.Values {
			if v == value {
				return false
			}
		}
		return true

	case v1.NodeSelectorOpExists:
		return exists

	case v1.NodeSelectorOpDoesNotExist:
		return !exists

	case v1.NodeSelectorOpGt, v1.NodeSelectorOpLt:
		// These require numeric comparison, skip for now
		return true
	}

	return true
}

// matchValue checks if a value matches a NodeSelectorRequirement
func matchValue(value string, req *v1.NodeSelectorRequirement) bool {
	switch req.Operator {
	case v1.NodeSelectorOpIn:
		for _, v := range req.Values {
			if v == value {
				return true
			}
		}
		return false

	case v1.NodeSelectorOpNotIn:
		for _, v := range req.Values {
			if v == value {
				return false
			}
		}
		return true
	}
	return true
}
