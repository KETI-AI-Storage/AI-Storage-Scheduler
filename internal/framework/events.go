package framework

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/utils/ptr"
)

const (
	ScheduleAttemptFailure = "ScheduleAttemptFailure"
	BackoffComplete        = "BackoffComplete"
	PopFromBackoffQ        = "PopFromBackoffQ"
	ForceActivate          = "ForceActivate"
	UnschedulableTimeout   = "UnschedulableTimeout"
)

var (
	EventAssignedPodAdd       = ClusterEvent{Resource: assignedPod, ActionType: Add}
	EventAssignedPodUpdate    = ClusterEvent{Resource: assignedPod, ActionType: Update}
	EventAssignedPodDelete    = ClusterEvent{Resource: assignedPod, ActionType: Delete}
	EventUnscheduledPodAdd    = ClusterEvent{Resource: unschedulablePod, ActionType: Add}
	EventUnscheduledPodUpdate = ClusterEvent{Resource: unschedulablePod, ActionType: Update}
	EventUnscheduledPodDelete = ClusterEvent{Resource: unschedulablePod, ActionType: Delete}
	EventUnschedulableTimeout = ClusterEvent{Resource: WildCard, ActionType: All, CustomLabel: UnschedulableTimeout}
	EventForceActivate        = ClusterEvent{Resource: WildCard, ActionType: All, CustomLabel: ForceActivate}
)

func PodSchedulingPropertiesChange(newPod *v1.Pod, oldPod *v1.Pod) (events []ClusterEvent) {
	r := assignedPod
	if newPod.Spec.NodeName == "" {
		r = unschedulablePod
	}

	podChangeExtractors := []podChangeExtractor{
		extractPodLabelsChange,
		extractPodScaleDown,
		extractPodSchedulingGateEliminatedChange,
		extractPodTolerationChange,
	}

	// if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
	// 	podChangeExtractors = append(podChangeExtractors, extractPodGeneratedResourceClaimChange)
	// }

	for _, fn := range podChangeExtractors {
		if event := fn(newPod, oldPod); event != None {
			events = append(events, ClusterEvent{Resource: r, ActionType: event})
		}
	}

	if len(events) == 0 {
		events = append(events, ClusterEvent{Resource: r, ActionType: Update})
	}

	return
}

type podChangeExtractor func(newPod *v1.Pod, oldPod *v1.Pod) ActionType

func extractPodLabelsChange(newPod *v1.Pod, oldPod *v1.Pod) ActionType {
	if isLabelChanged(newPod.GetLabels(), oldPod.GetLabels()) {
		return UpdatePodLabel
	}
	return None
}

func extractPodScaleDown(newPod, oldPod *v1.Pod) ActionType {
	opt := PodResourcesOptions{
		// UseStatusResources: true, // Pod.Status 기준
		UseStatusResources: false, //Pod.Spec 기준
	}
	newPodRequests := PodRequests(newPod, opt)
	oldPodRequests := PodRequests(oldPod, opt)

	for rName, oldReq := range oldPodRequests {
		newReq, ok := newPodRequests[rName]
		if !ok {
			return UpdatePodScaleDown
		}

		if oldReq.MilliValue() > newReq.MilliValue() {
			return UpdatePodScaleDown
		}
	}

	return None
}

func extractPodSchedulingGateEliminatedChange(newPod *v1.Pod, oldPod *v1.Pod) ActionType {
	if len(newPod.Spec.SchedulingGates) == 0 && len(oldPod.Spec.SchedulingGates) != 0 {
		return UpdatePodSchedulingGatesEliminated
	}

	return None
}

func extractPodTolerationChange(newPod *v1.Pod, oldPod *v1.Pod) ActionType {
	if len(newPod.Spec.Tolerations) != len(oldPod.Spec.Tolerations) {
		return UpdatePodToleration
	}

	return None
}

func PodStatusEqual(statusA, statusB []v1.PodResourceClaimStatus) bool {
	if len(statusA) != len(statusB) {
		return false
	}
	for i := range statusA {
		if statusA[i].Name != statusB[i].Name {
			return false
		}
		if !ptr.Equal(statusA[i].ResourceClaimName, statusB[i].ResourceClaimName) {
			return false
		}
	}
	return true
}

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
		if event := fn(newNode, oldNode); event != None {
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
	return None
}

func extractNodeLabelsChange(newNode *v1.Node, oldNode *v1.Node) ActionType {
	if isLabelChanged(newNode.GetLabels(), oldNode.GetLabels()) {
		return UpdateNodeLabel
	}
	return None
}

func isLabelChanged(newLabels map[string]string, oldLabels map[string]string) bool {
	return !equality.Semantic.DeepEqual(newLabels, oldLabels)
}

func extractNodeTaintsChange(newNode *v1.Node, oldNode *v1.Node) ActionType {
	if !equality.Semantic.DeepEqual(newNode.Spec.Taints, oldNode.Spec.Taints) {
		return UpdateNodeTaint
	}
	return None
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
	return None
}

func extractNodeSpecUnschedulableChange(newNode *v1.Node, oldNode *v1.Node) ActionType {
	if newNode.Spec.Unschedulable != oldNode.Spec.Unschedulable && !newNode.Spec.Unschedulable {
		return UpdateNodeTaint
	}
	return None
}

func extractNodeAnnotationsChange(newNode *v1.Node, oldNode *v1.Node) ActionType {
	if !equality.Semantic.DeepEqual(oldNode.GetAnnotations(), newNode.GetAnnotations()) {
		return UpdateNodeAnnotation
	}
	return None
}
