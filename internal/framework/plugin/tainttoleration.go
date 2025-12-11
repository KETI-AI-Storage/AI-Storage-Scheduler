package plugin

import (
	"context"
	"fmt"

	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

const TaintTolerationName = "TaintToleration"

// TaintToleration is a plugin that checks if a pod tolerates a node's taints.
type TaintToleration struct{}

var _ framework.FilterPlugin = &TaintToleration{}
var _ framework.ScorePlugin = &TaintToleration{}

func NewTaintToleration() *TaintToleration {
	return &TaintToleration{}
}

func (t *TaintToleration) Name() string {
	return TaintTolerationName
}

// Filter checks if a pod tolerates a node's taints
func (t *TaintToleration) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	node := nodeInfo.Node()

	// Get all taints that are not tolerated by the pod
	filterPredicate := func(taint *v1.Taint) bool {
		// Only consider NoSchedule and NoExecute taints
		return taint.Effect == v1.TaintEffectNoSchedule || taint.Effect == v1.TaintEffectNoExecute
	}

	taint, isUntolerated := FindMatchingUntoleratedTaint(node.Spec.Taints, pod.Spec.Tolerations, filterPredicate)
	if !isUntolerated {
		return utils.NewStatus(utils.Success, "")
	}

	return utils.NewStatus(utils.Unschedulable,
		fmt.Sprintf("node(s) had untolerated taint {%s: %s}", taint.Key, taint.Effect))
}

// Score gives a higher score to nodes with fewer taints that the pod doesn't tolerate
func (t *TaintToleration) Score(ctx context.Context, pod *v1.Pod, nodeName string) (int64, *utils.Status) {
	return 0, utils.NewStatus(utils.Success, "")
}

func (t *TaintToleration) ScoreExtensions() framework.ScoreExtensions {
	return t
}

func (t *TaintToleration) NormalizeScore(ctx context.Context, pod *v1.Pod, scores utils.PluginResult) *utils.Status {
	return utils.NewStatus(utils.Success, "")
}

// FindMatchingUntoleratedTaint checks if the given tolerations tolerate all taints that satisfy the filter predicate.
// It returns the first untolerated taint if found.
func FindMatchingUntoleratedTaint(taints []v1.Taint, tolerations []v1.Toleration, inclusionFilter func(*v1.Taint) bool) (v1.Taint, bool) {
	filteredTaints := []v1.Taint{}
	for i := range taints {
		if inclusionFilter != nil && !inclusionFilter(&taints[i]) {
			continue
		}
		filteredTaints = append(filteredTaints, taints[i])
	}

	for _, taint := range filteredTaints {
		if !TolerationsTolerateTaint(tolerations, &taint) {
			return taint, true
		}
	}
	return v1.Taint{}, false
}

// TolerationsTolerateTaint checks if taint is tolerated by any of the tolerations.
func TolerationsTolerateTaint(tolerations []v1.Toleration, taint *v1.Taint) bool {
	for i := range tolerations {
		if tolerations[i].ToleratesTaint(taint) {
			return true
		}
	}
	return false
}

// GetTolerationListFromPod gets all tolerations from a pod
func GetTolerationListFromPod(pod *v1.Pod) []v1.Toleration {
	if pod.Spec.Tolerations == nil {
		return []v1.Toleration{}
	}
	return pod.Spec.Tolerations
}

// TolerationsTolerateTaintsWithFilter checks if given tolerations tolerate all the taints that apply to the filter
func TolerationsTolerateTaintsWithFilter(tolerations []v1.Toleration, taints []v1.Taint, applyFilter func(*v1.Taint) bool) bool {
	for i := range taints {
		if applyFilter != nil && !applyFilter(&taints[i]) {
			continue
		}
		if !TolerationsTolerateTaint(tolerations, &taints[i]) {
			return false
		}
	}
	return true
}

// GetMatchingTolerations returns the list of matching tolerations
func GetMatchingTolerations(taints []v1.Taint, tolerations []v1.Toleration) ([]v1.Toleration, bool) {
	if len(taints) == 0 {
		return []v1.Toleration{}, true
	}
	if len(tolerations) == 0 && len(taints) > 0 {
		return []v1.Toleration{}, false
	}

	result := []v1.Toleration{}
	for i := range taints {
		tolerated := false
		for j := range tolerations {
			if tolerations[j].ToleratesTaint(&taints[i]) {
				result = append(result, tolerations[j])
				tolerated = true
				break
			}
		}
		if !tolerated {
			return []v1.Toleration{}, false
		}
	}
	return result, true
}

// FilterNoExecuteTaints filters out taints with NoExecute effect
func FilterNoExecuteTaints(taints []v1.Taint) []v1.Taint {
	result := []v1.Taint{}
	for i := range taints {
		if taints[i].Effect != v1.TaintEffectNoExecute {
			result = append(result, taints[i])
		}
	}
	return result
}

// GetTaintsWithEffect returns taints with specified effect
func GetTaintsWithEffect(taints []v1.Taint, effect v1.TaintEffect) []v1.Taint {
	result := []v1.Taint{}
	for i := range taints {
		if taints[i].Effect == effect {
			result = append(result, taints[i])
		}
	}
	return result
}

// GetTolerationKeys returns the set of toleration keys
func GetTolerationKeys(tolerations []v1.Toleration) sets.String {
	keys := sets.NewString()
	for i := range tolerations {
		keys.Insert(tolerations[i].Key)
	}
	return keys
}
