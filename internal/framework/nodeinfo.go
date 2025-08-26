package framework

import (
	"errors"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
)

// NodeInfo is node level aggregated information.
type NodeInfo struct {
	// Overall node information.
	node *v1.Node
	// Pods running on the node.
	Pods []*PodInfo
	// The subset of pods with affinity.
	PodsWithAffinity []*PodInfo
	// The subset of pods with required anti-affinity.
	PodsWithRequiredAntiAffinity []*PodInfo
	// Ports allocated on the node.
	UsedPorts HostPortInfo
	// Total requested resources of all pods on this node. This includes assumed
	// pods, which scheduler has sent for binding, but may not be scheduled yet.
	Requested *Resource
	// Total requested resources of all pods on this node with a minimum value
	// applied to each container's CPU and memory requests. This does not reflect
	// the actual resource requests for this node, but is used to avoid scheduling
	// many zero-request pods onto one node.
	NonZeroRequested *Resource
	// We store allocatedResources (which is Node.Status.Allocatable.*) explicitly
	// as int64, to avoid conversions and accessing map.
	Allocatable *Resource
	// ImageStates holds the entry of an image if and only if this image is on the node. The entry can be used for
	// checking an image's existence and advanced usage (e.g., image locality scheduling policy) based on the image
	// state information.
	ImageStates map[string]*ImageStateSummary
	// PVCRefCounts contains a mapping of PVC names to the number of pods on the node using it.
	// Keys are in the format "namespace/name".
	PVCRefCounts map[string]int
	// Whenever NodeInfo changes, generation is bumped.
	// This is used to avoid cloning it if the object didn't change.
	Generation int64
}

// NewNodeInfo returns a ready to use empty NodeInfo object.
// If any pods are given in arguments, their information will be aggregated in
// the returned object.
func NewNodeInfo(pods ...*v1.Pod) *NodeInfo {
	ni := &NodeInfo{
		Requested:        &Resource{},
		NonZeroRequested: &Resource{},
		Allocatable:      &Resource{},
		Generation:       nextGeneration(),
		UsedPorts:        make(HostPortInfo),
		ImageStates:      make(map[string]*ImageStateSummary),
		PVCRefCounts:     make(map[string]int),
	}
	for _, pod := range pods {
		ni.AddPod(pod)
	}
	return ni
}

// Node returns overall information about this node.
func (n *NodeInfo) Node() *v1.Node {
	if n == nil {
		return nil
	}
	return n.node
}

// Snapshot returns a copy of this node, Except that ImageStates is copied without the Nodes field.
func (n *NodeInfo) Snapshot() *NodeInfo {
	clone := &NodeInfo{
		node:             n.node,
		Requested:        n.Requested.Clone(),
		NonZeroRequested: n.NonZeroRequested.Clone(),
		Allocatable:      n.Allocatable.Clone(),
		UsedPorts:        make(HostPortInfo),
		ImageStates:      make(map[string]*ImageStateSummary),
		PVCRefCounts:     make(map[string]int),
		Generation:       n.Generation,
	}
	if len(n.Pods) > 0 {
		clone.Pods = append([]*PodInfo(nil), n.Pods...)
	}
	if len(n.UsedPorts) > 0 {
		// HostPortInfo is a map-in-map struct
		// make sure it's deep copied
		for ip, portMap := range n.UsedPorts {
			clone.UsedPorts[ip] = make(map[ProtocolPort]struct{})
			for protocolPort, v := range portMap {
				clone.UsedPorts[ip][protocolPort] = v
			}
		}
	}
	if len(n.PodsWithAffinity) > 0 {
		clone.PodsWithAffinity = append([]*PodInfo(nil), n.PodsWithAffinity...)
	}
	if len(n.PodsWithRequiredAntiAffinity) > 0 {
		clone.PodsWithRequiredAntiAffinity = append([]*PodInfo(nil), n.PodsWithRequiredAntiAffinity...)
	}
	if len(n.ImageStates) > 0 {
		state := make(map[string]*ImageStateSummary, len(n.ImageStates))
		for imageName, imageState := range n.ImageStates {
			state[imageName] = imageState.Snapshot()
		}
		clone.ImageStates = state
	}
	for key, value := range n.PVCRefCounts {
		clone.PVCRefCounts[key] = value
	}
	return clone
}

// String returns representation of human readable format of this NodeInfo.
func (n *NodeInfo) String() string {
	podKeys := make([]string, len(n.Pods))
	for i, p := range n.Pods {
		podKeys[i] = p.Pod.Name
	}
	return fmt.Sprintf("&NodeInfo{Pods:%v, RequestedResource:%#v, NonZeroRequest: %#v, UsedPort: %#v, AllocatableResource:%#v}",
		podKeys, n.Requested, n.NonZeroRequested, n.UsedPorts, n.Allocatable)
}

// AddPodInfo adds pod information to this NodeInfo.
// Consider using this instead of AddPod if a PodInfo is already computed.
func (n *NodeInfo) AddPodInfo(podInfo *PodInfo) {
	n.Pods = append(n.Pods, podInfo)
	if podWithAffinity(podInfo.Pod) {
		n.PodsWithAffinity = append(n.PodsWithAffinity, podInfo)
	}
	if podWithRequiredAntiAffinity(podInfo.Pod) {
		n.PodsWithRequiredAntiAffinity = append(n.PodsWithRequiredAntiAffinity, podInfo)
	}
	n.update(podInfo.Pod, 1)
}

// AddPod is a wrapper around AddPodInfo.
func (n *NodeInfo) AddPod(pod *v1.Pod) {
	// ignore this err since apiserver doesn't properly validate affinity terms
	// and we can't fix the validation for backwards compatibility.
	podInfo, _ := NewPodInfo(pod)
	n.AddPodInfo(podInfo)
}

func podWithAffinity(p *v1.Pod) bool {
	affinity := p.Spec.Affinity
	return affinity != nil && (affinity.PodAffinity != nil || affinity.PodAntiAffinity != nil)
}

func podWithRequiredAntiAffinity(p *v1.Pod) bool {
	affinity := p.Spec.Affinity
	return affinity != nil && affinity.PodAntiAffinity != nil &&
		len(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0
}

func removeFromSlice(logger klog.Logger, s []*PodInfo, k string) ([]*PodInfo, bool) {
	var removed bool
	for i := range s {
		tmpKey, err := GetPodKey(s[i].Pod)
		if err != nil {
			logger.Error(err, "Cannot get pod key", "pod", klog.KObj(s[i].Pod))
			continue
		}
		if k == tmpKey {
			// delete the element
			s[i] = s[len(s)-1]
			s = s[:len(s)-1]
			removed = true
			break
		}
	}
	// resets the slices to nil so that we can do DeepEqual in unit tests.
	if len(s) == 0 {
		return nil, removed
	}
	return s, removed
}

// RemovePod subtracts pod information from this NodeInfo.
func (n *NodeInfo) RemovePod(logger klog.Logger, pod *v1.Pod) error {
	k, err := GetPodKey(pod)
	if err != nil {
		return err
	}
	if podWithAffinity(pod) {
		n.PodsWithAffinity, _ = removeFromSlice(logger, n.PodsWithAffinity, k)
	}
	if podWithRequiredAntiAffinity(pod) {
		n.PodsWithRequiredAntiAffinity, _ = removeFromSlice(logger, n.PodsWithRequiredAntiAffinity, k)
	}

	var removed bool
	if n.Pods, removed = removeFromSlice(logger, n.Pods, k); removed {
		n.update(pod, -1)
		return nil
	}
	return fmt.Errorf("no corresponding pod %s in pods of node %s", pod.Name, n.node.Name)
}

// update node info based on the pod and sign.
// The sign will be set to `+1` when AddPod and to `-1` when RemovePod.
func (n *NodeInfo) update(pod *v1.Pod, sign int64) {
	res, non0CPU, non0Mem := calculateResource(pod)
	n.Requested.MilliCPU += sign * res.MilliCPU
	n.Requested.Memory += sign * res.Memory
	n.Requested.EphemeralStorage += sign * res.EphemeralStorage
	if n.Requested.ScalarResources == nil && len(res.ScalarResources) > 0 {
		n.Requested.ScalarResources = map[v1.ResourceName]int64{}
	}
	for rName, rQuant := range res.ScalarResources {
		n.Requested.ScalarResources[rName] += sign * rQuant
	}
	n.NonZeroRequested.MilliCPU += sign * non0CPU
	n.NonZeroRequested.Memory += sign * non0Mem

	// Consume ports when pod added or release ports when pod removed.
	n.updateUsedPorts(pod, sign > 0)
	n.updatePVCRefCounts(pod, sign > 0)

	n.Generation = nextGeneration()
}

func getNonMissingContainerRequests(requests v1.ResourceList, podLevelResourcesSet bool) v1.ResourceList {
	if !podLevelResourcesSet {
		return v1.ResourceList{
			v1.ResourceCPU:    *resource.NewMilliQuantity(schedutil.DefaultMilliCPURequest, resource.DecimalSI),
			v1.ResourceMemory: *resource.NewQuantity(schedutil.DefaultMemoryRequest, resource.DecimalSI),
		}
	}

	nonMissingContainerRequests := make(v1.ResourceList, 2)
	if _, exists := requests[v1.ResourceCPU]; !exists {
		nonMissingContainerRequests[v1.ResourceCPU] = *resource.NewMilliQuantity(schedutil.DefaultMilliCPURequest, resource.DecimalSI)
	}

	if _, exists := requests[v1.ResourceMemory]; !exists {
		nonMissingContainerRequests[v1.ResourceMemory] = *resource.NewQuantity(schedutil.DefaultMemoryRequest, resource.DecimalSI)
	}
	return nonMissingContainerRequests

}

func calculateResource(pod *v1.Pod) (Resource, int64, int64) {
	requests := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{
		UseStatusResources: utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling),
		// SkipPodLevelResources is set to false when PodLevelResources feature is enabled.
		SkipPodLevelResources: !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources),
	})
	isPodLevelResourcesSet := utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources) && resourcehelper.IsPodLevelRequestsSet(pod)
	nonMissingContainerRequests := getNonMissingContainerRequests(requests, isPodLevelResourcesSet)
	non0Requests := requests
	if len(nonMissingContainerRequests) > 0 {
		non0Requests = resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{
			UseStatusResources: utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling),
			// SkipPodLevelResources is set to false when PodLevelResources feature is enabled.
			SkipPodLevelResources:       !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources),
			NonMissingContainerRequests: nonMissingContainerRequests,
		})
	}
	non0CPU := non0Requests[v1.ResourceCPU]
	non0Mem := non0Requests[v1.ResourceMemory]

	var res Resource
	res.Add(requests)
	return res, non0CPU.MilliValue(), non0Mem.Value()
}

func GetPodKey(pod *v1.Pod) (string, error) {
	uid := string(pod.UID)
	if len(uid) == 0 {
		return "", errors.New("cannot get cache key for pod with empty UID")
	}
	return uid, nil
}

func GetNamespacedName(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

// updateUsedPorts updates the UsedPorts of NodeInfo.
func (n *NodeInfo) updateUsedPorts(pod *v1.Pod, add bool) {
	for _, container := range pod.Spec.Containers {
		for _, podPort := range container.Ports {
			if add {
				n.UsedPorts.Add(podPort.HostIP, string(podPort.Protocol), podPort.HostPort)
			} else {
				n.UsedPorts.Remove(podPort.HostIP, string(podPort.Protocol), podPort.HostPort)
			}
		}
	}
}

// updatePVCRefCounts updates the PVCRefCounts of NodeInfo.
func (n *NodeInfo) updatePVCRefCounts(pod *v1.Pod, add bool) {
	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim == nil {
			continue
		}

		key := GetNamespacedName(pod.Namespace, v.PersistentVolumeClaim.ClaimName)
		if add {
			n.PVCRefCounts[key] += 1
		} else {
			n.PVCRefCounts[key] -= 1
			if n.PVCRefCounts[key] <= 0 {
				delete(n.PVCRefCounts, key)
			}
		}
	}
}

// SetNode sets the overall node information.
func (n *NodeInfo) SetNode(node *v1.Node) {
	n.node = node
	n.Allocatable = NewResource(node.Status.Allocatable)
	n.Generation = nextGeneration()
}

// RemoveNode removes the node object, leaving all other tracking information.
func (n *NodeInfo) RemoveNode() {
	n.node = nil
	n.Generation = nextGeneration()
}
