package scheduler

import (
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
)

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

var (
	// basicActionTypes is a list of basicActionTypes ActionTypes.
	basicActionTypes = []ActionType{Add, Delete, Update}
	// podActionTypes is a list of ActionTypes that are only applicable for Pod events.
	podActionTypes = []ActionType{UpdatePodLabel, UpdatePodScaleDown, UpdatePodTolerations, UpdatePodSchedulingGatesEliminated, UpdatePodGeneratedResourceClaim}
	// nodeActionTypes is a list of ActionTypes that are only applicable for Node events.
	nodeActionTypes = []ActionType{UpdateNodeAllocatable, UpdateNodeLabel, UpdateNodeTaint, UpdateNodeCondition, UpdateNodeAnnotation}
)

type EventResource string

type ClusterEvent struct {
	Resource   EventResource
	ActionType ActionType

	// label describes this cluster event.
	// It's an optional field to control String(), which is used in logging and metrics.
	// Normally, it's not necessary to set this field; only used for special events like UnschedulableTimeout.
	label string
}

const (
	Pod                   EventResource = "Pod"
	AssignedPod           EventResource = "AssignedPod"
	UnschedulablePod      EventResource = "UnschedulablePod"
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
		AssignedPod,
		UnschedulablePod,
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

type podChangeExtractor func(newPod *v1.Pod, oldPod *v1.Pod) ActionType

func PodSchedulingPropertiesChange(newPod *v1.Pod, oldPod *v1.Pod) (events []ClusterEvent) {
	r := AssignedPod
	if newPod.Spec.NodeName == "" {
		r = UnschedulablePod
	}

	podChangeExtracters := []podChangeExtractor{
		extractPodLabelsChange,
		extractPodScaleDown,
		extractPodSchedulingGateEliminatedChange,
		extractPodTolerationChange,
	}

	for _, fn := range podChangeExtracters {
		if event := fn(newPod, oldPod); event != none {
			events = append(events, ClusterEvent{Resource: r, ActionType: event})
		}
	}

	if len(events) == 0 {
		// When no specific event is found, we use AssignedPodOtherUpdate,
		// which should only trigger plugins registering a general Pod/Update event.
		events = append(events, ClusterEvent{Resource: r, ActionType: updatePodOther})
	}

	return
}

func extractPodScaleDown(newPod, oldPod *v1.Pod) ActionType {

	newPodRequests := utils.PodRequests(newPod)
	oldPodRequests := utils.PodRequests(oldPod)

	for rName, oldReq := range oldPodRequests {
		newReq, ok := newPodRequests[rName]
		if !ok {
			// The resource request of rName is removed.
			return UpdatePodScaleDown
		}

		if oldReq.MilliValue() > newReq.MilliValue() {
			// The resource request of rName is scaled down.
			return UpdatePodScaleDown
		}
	}

	return none
}

func extractPodLabelsChange(newPod *v1.Pod, oldPod *v1.Pod) ActionType {
	if isLabelChanged(newPod.GetLabels(), oldPod.GetLabels()) {
		return UpdatePodLabel
	}
	return none
}

func isLabelChanged(newLabels map[string]string, oldLabels map[string]string) bool {
	return !equality.Semantic.DeepEqual(newLabels, oldLabels)
}

func extractPodTolerationChange(newPod *v1.Pod, oldPod *v1.Pod) ActionType {
	if len(newPod.Spec.Tolerations) != len(oldPod.Spec.Tolerations) {
		// A Pod got a new toleration.
		// Due to API validation, the user can add, but cannot modify or remove tolerations.
		// So, it's enough to just check the length of tolerations to notice the update.
		// And, any updates in tolerations could make Pod schedulable.
		return UpdatePodTolerations
	}

	return none
}

func extractPodSchedulingGateEliminatedChange(newPod *v1.Pod, oldPod *v1.Pod) ActionType {
	if len(newPod.Spec.SchedulingGates) == 0 && len(oldPod.Spec.SchedulingGates) != 0 {
		// A scheduling gate on the pod is completely removed.
		return UpdatePodSchedulingGatesEliminated
	}

	return none
}

const (
	// ScheduleAttemptFailure is the event when a schedule attempt fails.
	ScheduleAttemptFailure = "ScheduleAttemptFailure"
	// BackoffComplete is the event when a pod finishes backoff.
	BackoffComplete = "BackoffComplete"
	// ForceActivate is the event when a pod is moved from unschedulablePods/backoffQ
	// to activeQ. Usually it's triggered by plugin implementations.
	ForceActivate = "ForceActivate"
	// UnschedulableTimeout is the event when a pod is moved from unschedulablePods
	// due to the timeout specified at pod-max-in-unschedulable-pods-duration.
	UnschedulableTimeout = "UnschedulableTimeout"
)

var (
	// EventAssignedPodAdd is the event when an assigned pod is added.
	EventAssignedPodAdd = ClusterEvent{Resource: AssignedPod, ActionType: Add}
	// EventAssignedPodUpdate is the event when an assigned pod is updated.
	EventAssignedPodUpdate = ClusterEvent{Resource: AssignedPod, ActionType: Update}
	// EventAssignedPodDelete is the event when an assigned pod is deleted.
	EventAssignedPodDelete = ClusterEvent{Resource: AssignedPod, ActionType: Delete}
	// EventUnscheduledPodAdd is the event when an unscheduled pod is added.
	EventUnscheduledPodAdd = ClusterEvent{Resource: UnschedulablePod, ActionType: Add}
	// EventUnscheduledPodUpdate is the event when an unscheduled pod is updated.
	EventUnscheduledPodUpdate = ClusterEvent{Resource: UnschedulablePod, ActionType: Update}
	// EventUnscheduledPodDelete is the event when an unscheduled pod is deleted.
	EventUnscheduledPodDelete = ClusterEvent{Resource: UnschedulablePod, ActionType: Delete}
	// EventUnschedulableTimeout is the event when a pod stays in unschedulable for longer than timeout.
	EventUnschedulableTimeout = ClusterEvent{Resource: WildCard, ActionType: All, label: UnschedulableTimeout}
	// EventForceActivate is the event when a pod is moved from unschedulablePods/backoffQ to activeQ.
	EventForceActivate = ClusterEvent{Resource: WildCard, ActionType: All, label: ForceActivate}
)

// NodeSchedulingPropertiesChange interprets the update of a node and returns corresponding UpdateNodeXYZ event(s).
func NodeSchedulingPropertiesChange(newNode *v1.Node, oldNode *v1.Node) (events []ClusterEvent) {
	nodeChangeExtracters := []nodeChangeExtractor{
		extractNodeSpecUnschedulableChange,
		extractNodeAllocatableChange,
		extractNodeLabelsChange,
		extractNodeTaintsChange,
		extractNodeConditionsChange,
		extractNodeAnnotationsChange,
	}

	for _, fn := range nodeChangeExtracters {
		if event := fn(newNode, oldNode); event != none {
			events = append(events, ClusterEvent{Resource: Node, ActionType: event})
		}
	}
	return
}

type nodeChangeExtractor func(newNode *v1.Node, oldNode *v1.Node) ActionType

func extractNodeAllocatableChange(newNode *v1.Node, oldNode *v1.Node) ActionType {
	if !equality.Semantic.DeepEqual(oldNode.Status.Allocatable, newNode.Status.Allocatable) {
		return UpdateNodeAllocatable
	}
	return none
}

func extractNodeLabelsChange(newNode *v1.Node, oldNode *v1.Node) ActionType {
	if isLabelChanged(newNode.GetLabels(), oldNode.GetLabels()) {
		return UpdateNodeLabel
	}
	return none
}

func extractNodeTaintsChange(newNode *v1.Node, oldNode *v1.Node) ActionType {
	if !equality.Semantic.DeepEqual(newNode.Spec.Taints, oldNode.Spec.Taints) {
		return UpdateNodeTaint
	}
	return none
}

func extractNodeConditionsChange(newNode *v1.Node, oldNode *v1.Node) ActionType {
	strip := func(conditions []v1.NodeCondition) map[v1.NodeConditionType]v1.ConditionStatus {
		conditionStatuses := make(map[v1.NodeConditionType]v1.ConditionStatus, len(conditions))
		for i := range conditions {
			conditionStatuses[conditions[i].Type] = conditions[i].Status
		}
		return conditionStatuses
	}
	if !equality.Semantic.DeepEqual(strip(oldNode.Status.Conditions), strip(newNode.Status.Conditions)) {
		return UpdateNodeCondition
	}
	return none
}

func extractNodeSpecUnschedulableChange(newNode *v1.Node, oldNode *v1.Node) ActionType {
	if newNode.Spec.Unschedulable != oldNode.Spec.Unschedulable && !newNode.Spec.Unschedulable {
		// TODO: create UpdateNodeSpecUnschedulable ActionType
		return UpdateNodeTaint
	}
	return none
}

func extractNodeAnnotationsChange(newNode *v1.Node, oldNode *v1.Node) ActionType {
	if !equality.Semantic.DeepEqual(oldNode.GetAnnotations(), newNode.GetAnnotations()) {
		return UpdateNodeAnnotation
	}
	return none
}
