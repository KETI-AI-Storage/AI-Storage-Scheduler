package plugin

import (
	"context"
	"fmt"
	"sync"

	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const InterPodAffinityName = "InterPodAffinity"

// InterPodAffinity is a plugin that checks inter-pod affinity and anti-affinity constraints.
type InterPodAffinity struct {
	cache *utils.Cache

	// preFilterState stores the precomputed data for scoring
	preFilterState     map[string]*preFilterState
	preFilterStateLock sync.RWMutex
}

type preFilterState struct {
	// topologyToMatchedTermCount maps topology key to the count of matching pod anti-affinity terms
	topologyToMatchedAffinityTermCount     map[string]map[string]int64
	topologyToMatchedAntiAffinityTermCount map[string]map[string]int64
}

var _ framework.PreFilterPlugin = &InterPodAffinity{}
var _ framework.FilterPlugin = &InterPodAffinity{}
var _ framework.PreScorePlugin = &InterPodAffinity{}
var _ framework.ScorePlugin = &InterPodAffinity{}

func NewInterPodAffinity(cache *utils.Cache) *InterPodAffinity {
	return &InterPodAffinity{
		cache:          cache,
		preFilterState: make(map[string]*preFilterState),
	}
}

func (p *InterPodAffinity) Name() string {
	return InterPodAffinityName
}

// PreFilter computes affinity terms
func (p *InterPodAffinity) PreFilter(ctx context.Context, pod *v1.Pod) (*framework.PreFilterResult, *utils.Status) {
	affinity := pod.Spec.Affinity
	if affinity == nil || (affinity.PodAffinity == nil && affinity.PodAntiAffinity == nil) {
		return nil, utils.NewStatus(utils.Success, "")
	}

	// Store state for later use
	state := &preFilterState{
		topologyToMatchedAffinityTermCount:     make(map[string]map[string]int64),
		topologyToMatchedAntiAffinityTermCount: make(map[string]map[string]int64),
	}

	podKey, _ := utils.GetPodKey(pod)
	p.preFilterStateLock.Lock()
	p.preFilterState[podKey] = state
	p.preFilterStateLock.Unlock()

	return nil, utils.NewStatus(utils.Success, "")
}

func (p *InterPodAffinity) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// Filter checks if the pod's affinity/anti-affinity constraints are satisfied by the node
func (p *InterPodAffinity) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	node := nodeInfo.Node()
	affinity := pod.Spec.Affinity

	if affinity == nil {
		return utils.NewStatus(utils.Success, "")
	}

	// Check pod affinity
	if affinity.PodAffinity != nil {
		for _, term := range affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			if !p.satisfiesAffinityTerm(pod, nodeInfo, &term) {
				return utils.NewStatus(utils.Unschedulable,
					fmt.Sprintf("node does not satisfy pod affinity term: %s", term.TopologyKey))
			}
		}
	}

	// Check pod anti-affinity
	if affinity.PodAntiAffinity != nil {
		for _, term := range affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			if p.existsMatchingPod(pod, nodeInfo, &term) {
				return utils.NewStatus(utils.Unschedulable,
					fmt.Sprintf("node has pod matching anti-affinity term: %s", term.TopologyKey))
			}
		}
	}

	// Check if existing pods on the node have anti-affinity against this pod
	for _, existingPodInfo := range nodeInfo.PodsWithRequiredAntiAffinity {
		existingPod := existingPodInfo.Pod
		if existingPod.Spec.Affinity == nil || existingPod.Spec.Affinity.PodAntiAffinity == nil {
			continue
		}
		for _, term := range existingPod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			if p.podMatchesTerm(pod, existingPod, &term, node) {
				return utils.NewStatus(utils.Unschedulable,
					fmt.Sprintf("existing pod %s/%s has anti-affinity against the pod",
						existingPod.Namespace, existingPod.Name))
			}
		}
	}

	return utils.NewStatus(utils.Success, "")
}

// satisfiesAffinityTerm checks if the node satisfies an affinity term
func (p *InterPodAffinity) satisfiesAffinityTerm(pod *v1.Pod, nodeInfo *utils.NodeInfo, term *v1.PodAffinityTerm) bool {
	node := nodeInfo.Node()
	topologyValue, exists := node.Labels[term.TopologyKey]
	if !exists {
		return false
	}

	// Check if any pod on this node matches the affinity term
	for _, existingPodInfo := range nodeInfo.Pods {
		existingPod := existingPodInfo.Pod
		if p.podMatchesAffinityTerm(existingPod, term, topologyValue) {
			return true
		}
	}

	// Check pods in other nodes with same topology
	if p.cache != nil {
		for _, ni := range p.cache.Nodes() {
			if ni.Node() == nil {
				continue
			}
			if tv, ok := ni.Node().Labels[term.TopologyKey]; ok && tv == topologyValue {
				for _, pi := range ni.Pods {
					if p.podMatchesAffinityTerm(pi.Pod, term, topologyValue) {
						return true
					}
				}
			}
		}
	}

	return false
}

// existsMatchingPod checks if there's a pod matching the anti-affinity term
func (p *InterPodAffinity) existsMatchingPod(pod *v1.Pod, nodeInfo *utils.NodeInfo, term *v1.PodAffinityTerm) bool {
	node := nodeInfo.Node()
	topologyValue, exists := node.Labels[term.TopologyKey]
	if !exists {
		return false
	}

	// Check pods on this node
	for _, existingPodInfo := range nodeInfo.Pods {
		if p.podMatchesAffinityTerm(existingPodInfo.Pod, term, topologyValue) {
			return true
		}
	}

	// Check pods on other nodes with same topology
	if p.cache != nil {
		for _, ni := range p.cache.Nodes() {
			if ni.Node() == nil {
				continue
			}
			if tv, ok := ni.Node().Labels[term.TopologyKey]; ok && tv == topologyValue {
				for _, pi := range ni.Pods {
					if p.podMatchesAffinityTerm(pi.Pod, term, topologyValue) {
						return true
					}
				}
			}
		}
	}

	return false
}

// podMatchesTerm checks if the pod matches an anti-affinity term from an existing pod
func (p *InterPodAffinity) podMatchesTerm(pod, existingPod *v1.Pod, term *v1.PodAffinityTerm, node *v1.Node) bool {
	topologyValue, exists := node.Labels[term.TopologyKey]
	if !exists {
		return false
	}
	return p.podMatchesAffinityTerm(pod, term, topologyValue)
}

// podMatchesAffinityTerm checks if a pod matches an affinity term
func (p *InterPodAffinity) podMatchesAffinityTerm(pod *v1.Pod, term *v1.PodAffinityTerm, topologyValue string) bool {
	// Check namespace match
	namespaces := make(map[string]struct{})
	if term.NamespaceSelector != nil {
		// For simplicity, we assume namespace selector selects all namespaces
		// In a full implementation, this would need namespace listing
		namespaces[pod.Namespace] = struct{}{}
	} else if len(term.Namespaces) > 0 {
		for _, ns := range term.Namespaces {
			namespaces[ns] = struct{}{}
		}
	} else {
		// Default to pod's namespace
		namespaces[pod.Namespace] = struct{}{}
	}

	if _, ok := namespaces[pod.Namespace]; !ok {
		return false
	}

	// Check label selector match
	if term.LabelSelector == nil {
		return true
	}

	selector, err := metav1.LabelSelectorAsSelector(term.LabelSelector)
	if err != nil {
		return false
	}

	return selector.Matches(labels.Set(pod.Labels))
}

// PreScore computes data for scoring
func (p *InterPodAffinity) PreScore(ctx context.Context, pod *v1.Pod, nodes []*v1.Node) *utils.Status {
	return utils.NewStatus(utils.Success, "")
}

// Score gives a higher score to nodes with more matching affinity pods and fewer anti-affinity pods
func (p *InterPodAffinity) Score(ctx context.Context, pod *v1.Pod, nodeName string) (int64, *utils.Status) {
	if p.cache == nil {
		return 0, utils.NewStatus(utils.Success, "")
	}

	nodeInfo := p.cache.Nodes()[nodeName]
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return 0, utils.NewStatus(utils.Error, "node not found in cache")
	}

	affinity := pod.Spec.Affinity
	if affinity == nil {
		return 0, utils.NewStatus(utils.Success, "")
	}

	var score int64

	// Score based on preferred pod affinity
	if affinity.PodAffinity != nil {
		for _, term := range affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			if p.satisfiesAffinityTerm(pod, nodeInfo, &term.PodAffinityTerm) {
				score += int64(term.Weight)
			}
		}
	}

	// Score based on preferred pod anti-affinity (subtract for matches)
	if affinity.PodAntiAffinity != nil {
		for _, term := range affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			if p.existsMatchingPod(pod, nodeInfo, &term.PodAffinityTerm) {
				score -= int64(term.Weight)
			}
		}
	}

	// Normalize to 0-100 range
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}

	return score, utils.NewStatus(utils.Success, "")
}

func (p *InterPodAffinity) ScoreExtensions() framework.ScoreExtensions {
	return p
}

func (p *InterPodAffinity) NormalizeScore(ctx context.Context, pod *v1.Pod, scores utils.PluginResult) *utils.Status {
	return utils.NewStatus(utils.Success, "")
}
