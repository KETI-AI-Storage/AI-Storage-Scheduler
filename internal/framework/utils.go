package framework

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

var generation int64

type ActionType int64

const (
	Add ActionType = 1 << iota
	Delete
	UpdateNodeAllocatable
	UpdateNodeLabel
	UpdateNodeTaint
	UpdateNodeCondition
	UpdateNodeAnnotation
	UpdatePodLabel
	UpdatePodScaleDown
	UpdatePodTolerations
	UpdatePodSchedulingGatesEliminated
	UpdatePodGeneratedResourceClaim
	updatePodOther
	All    ActionType = 1<<iota - 1
	Update            = UpdateNodeAllocatable | UpdateNodeLabel | UpdateNodeTaint | UpdateNodeCondition | UpdateNodeAnnotation | UpdatePodLabel | UpdatePodScaleDown | UpdatePodTolerations | UpdatePodSchedulingGatesEliminated | UpdatePodGeneratedResourceClaim | updatePodOther
	none   ActionType = 0
)

var (
	basicActionTypes = []ActionType{Add, Delete, Update}
	podActionTypes   = []ActionType{UpdatePodLabel, UpdatePodScaleDown, UpdatePodTolerations, UpdatePodSchedulingGatesEliminated, UpdatePodGeneratedResourceClaim}
	nodeActionTypes  = []ActionType{UpdateNodeAllocatable, UpdateNodeLabel, UpdateNodeTaint, UpdateNodeCondition, UpdateNodeAnnotation}
)

func (a ActionType) String() string {
	switch a {
	case Add:
		return "Add"
	case Delete:
		return "Delete"
	case UpdateNodeAllocatable:
		return "UpdateNodeAllocatable"
	case UpdateNodeLabel:
		return "UpdateNodeLabel"
	case UpdateNodeTaint:
		return "UpdateNodeTaint"
	case UpdateNodeCondition:
		return "UpdateNodeCondition"
	case UpdateNodeAnnotation:
		return "UpdateNodeAnnotation"
	case UpdatePodLabel:
		return "UpdatePodLabel"
	case UpdatePodScaleDown:
		return "UpdatePodScaleDown"
	case UpdatePodTolerations:
		return "UpdatePodTolerations"
	case UpdatePodSchedulingGatesEliminated:
		return "UpdatePodSchedulingGatesEliminated"
	case UpdatePodGeneratedResourceClaim:
		return "UpdatePodGeneratedResourceClaim"
	case updatePodOther:
		return "Update"
	case All:
		return "All"
	case Update:
		return "Update"
	}

	// Shouldn't reach here.
	return ""
}

type EventResource string

const (
	Pod                   EventResource = "Pod"
	assignedPod           EventResource = "AssignedPod"
	unschedulablePod      EventResource = "UnschedulablePod"
	Node                  EventResource = "Node"
	PersistentVolume      EventResource = "PersistentVolume"
	PersistentVolumeClaim EventResource = "PersistentVolumeClaim"
	CSINode               EventResource = "storage.k8s.io/CSINode"
	CSIDriver             EventResource = "storage.k8s.io/CSIDriver"
	VolumeAttachment      EventResource = "storage.k8s.io/VolumeAttachment"
	CSIStorageCapacity    EventResource = "storage.k8s.io/CSIStorageCapacity"
	StorageClass          EventResource = "storage.k8s.io/StorageClass"
	ResourceClaim         EventResource = "resource.k8s.io/ResourceClaim"
	ResourceSlice         EventResource = "resource.k8s.io/ResourceSlice"
	DeviceClass           EventResource = "resource.k8s.io/DeviceClass"
	WildCard              EventResource = "*"
)

var (
	// allResources is a list of all resources.
	allResources = []EventResource{
		Pod,
		assignedPod,
		unschedulablePod,
		Node,
		PersistentVolume,
		PersistentVolumeClaim,
		CSINode,
		CSIDriver,
		CSIStorageCapacity,
		StorageClass,
		VolumeAttachment,
		ResourceClaim,
		ResourceSlice,
		DeviceClass,
	}
)

type ClusterEventWithHint struct {
	Event          ClusterEvent
	QueueingHintFn QueueingHintFn
}

type QueueingHintFn func(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (QueueingHint, error)

type QueueingHint int

const (
	QueueSkip QueueingHint = iota
	Queue
)

func (s QueueingHint) String() string {
	switch s {
	case QueueSkip:
		return "QueueSkip"
	case Queue:
		return "Queue"
	}
	return ""
}

type ClusterEvent struct {
	Resource   EventResource
	ActionType ActionType
	label      string
}

func (ce ClusterEvent) Label() string {
	if ce.label != "" {
		return ce.label
	}

	return fmt.Sprintf("%v%v", ce.Resource, ce.ActionType)
}

func AllClusterEventLabels() []string {
	labels := []string{UnschedulableTimeout, ForceActivate}
	for _, r := range allResources {
		for _, a := range basicActionTypes {
			labels = append(labels, ClusterEvent{Resource: r, ActionType: a}.Label())
		}
		if r == Pod {
			for _, a := range podActionTypes {
				labels = append(labels, ClusterEvent{Resource: r, ActionType: a}.Label())
			}
		} else if r == Node {
			for _, a := range nodeActionTypes {
				labels = append(labels, ClusterEvent{Resource: r, ActionType: a}.Label())
			}
		}
	}
	return labels
}

func (ce ClusterEvent) IsWildCard() bool {
	return ce.Resource == WildCard && ce.ActionType == All
}

func (ce ClusterEvent) Match(incomingEvent ClusterEvent) bool {
	return ce.IsWildCard() || ce.Resource.match(incomingEvent.Resource) && ce.ActionType&incomingEvent.ActionType != 0
}

func (r EventResource) match(resource EventResource) bool {
	return r == WildCard ||
		r == resource ||
		r == Pod && (resource == assignedPod || resource == unschedulablePod)
}

func UnrollWildCardResource() []ClusterEventWithHint {
	return []ClusterEventWithHint{
		{Event: ClusterEvent{Resource: Pod, ActionType: All}},
		{Event: ClusterEvent{Resource: Node, ActionType: All}},
		{Event: ClusterEvent{Resource: PersistentVolume, ActionType: All}},
		{Event: ClusterEvent{Resource: PersistentVolumeClaim, ActionType: All}},
		{Event: ClusterEvent{Resource: CSINode, ActionType: All}},
		{Event: ClusterEvent{Resource: CSIDriver, ActionType: All}},
		{Event: ClusterEvent{Resource: CSIStorageCapacity, ActionType: All}},
		{Event: ClusterEvent{Resource: StorageClass, ActionType: All}},
		{Event: ClusterEvent{Resource: ResourceClaim, ActionType: All}},
		{Event: ClusterEvent{Resource: DeviceClass, ActionType: All}},
	}
}
