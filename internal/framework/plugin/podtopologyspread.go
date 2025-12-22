package plugin

import (
	"context"
	"fmt"
	"math"
	"sync"

	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const PodTopologySpreadName = "PodTopologySpread"

// PodTopologySpread is a plugin that ensures pods are evenly spread across topology domains.
type PodTopologySpread struct {
	cache *utils.Cache

	// preFilterState stores precomputed topology data
	preFilterState     map[string]*topologySpreadState
	preFilterStateLock sync.RWMutex
}

type topologySpreadState struct {
	// constraints from the pod
	constraints []topologySpreadConstraint
	// topologyPairToPodCounts maps topology key/value pairs to pod counts
	topologyPairToPodCounts map[topologyPair]int
}

type topologySpreadConstraint struct {
	maxSkew            int32
	topologyKey        string
	whenUnsatisfiable  v1.UnsatisfiableConstraintAction
	selector           labels.Selector
	minDomains         int32
	nodeAffinityPolicy v1.NodeInclusionPolicy
	nodeTaintsPolicy   v1.NodeInclusionPolicy
	matchLabelKeys     []string
}

type topologyPair struct {
	key   string
	value string
}

var _ framework.PreFilterPlugin = &PodTopologySpread{}
var _ framework.FilterPlugin = &PodTopologySpread{}
var _ framework.PreScorePlugin = &PodTopologySpread{}
var _ framework.ScorePlugin = &PodTopologySpread{}

func NewPodTopologySpread(cache *utils.Cache) *PodTopologySpread {
	return &PodTopologySpread{
		cache:          cache,
		preFilterState: make(map[string]*topologySpreadState),
	}
}

func (p *PodTopologySpread) Name() string {
	return PodTopologySpreadName
}

// PreFilter computes the topology spread constraints
func (p *PodTopologySpread) PreFilter(ctx context.Context, pod *v1.Pod) (*framework.PreFilterResult, *utils.Status) {
	constraints := pod.Spec.TopologySpreadConstraints
	if len(constraints) == 0 {
		return nil, utils.NewStatus(utils.Success, "")
	}

	state := &topologySpreadState{
		constraints:             make([]topologySpreadConstraint, 0),
		topologyPairToPodCounts: make(map[topologyPair]int),
	}

	for _, c := range constraints {
		selector, err := metav1.LabelSelectorAsSelector(c.LabelSelector)
		if err != nil {
			continue
		}

		tsc := topologySpreadConstraint{
			maxSkew:           c.MaxSkew,
			topologyKey:       c.TopologyKey,
			whenUnsatisfiable: c.WhenUnsatisfiable,
			selector:          selector,
		}
		if c.MinDomains != nil {
			tsc.minDomains = *c.MinDomains
		}
		state.constraints = append(state.constraints, tsc)
	}

	// Count matching pods per topology domain
	if p.cache != nil {
		for _, nodeInfo := range p.cache.Nodes() {
			if nodeInfo.Node() == nil {
				continue
			}
			node := nodeInfo.Node()

			for _, c := range state.constraints {
				topologyValue, exists := node.Labels[c.topologyKey]
				if !exists {
					continue
				}

				pair := topologyPair{key: c.topologyKey, value: topologyValue}

				for _, podInfo := range nodeInfo.Pods {
					if c.selector.Matches(labels.Set(podInfo.Pod.Labels)) {
						state.topologyPairToPodCounts[pair]++
					}
				}
			}
		}
	}

	podKey, _ := utils.GetPodKey(pod)
	p.preFilterStateLock.Lock()
	p.preFilterState[podKey] = state
	p.preFilterStateLock.Unlock()

	return nil, utils.NewStatus(utils.Success, "")
}

func (p *PodTopologySpread) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// Filter checks if placing the pod on the node would violate topology spread constraints
func (p *PodTopologySpread) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	constraints := pod.Spec.TopologySpreadConstraints
	if len(constraints) == 0 {
		return utils.NewStatus(utils.Success, "")
	}

	node := nodeInfo.Node()

	podKey, _ := utils.GetPodKey(pod)
	p.preFilterStateLock.RLock()
	state := p.preFilterState[podKey]
	p.preFilterStateLock.RUnlock()

	if state == nil {
		// Recompute if state is not available
		p.PreFilter(ctx, pod)
		p.preFilterStateLock.RLock()
		state = p.preFilterState[podKey]
		p.preFilterStateLock.RUnlock()
	}

	if state == nil {
		return utils.NewStatus(utils.Success, "")
	}

	for _, c := range state.constraints {
		if c.whenUnsatisfiable != v1.DoNotSchedule {
			continue
		}

		topologyValue, exists := node.Labels[c.topologyKey]
		if !exists {
			return utils.NewStatus(utils.Unschedulable,
				fmt.Sprintf("node doesn't have required topology key: %s", c.topologyKey))
		}

		pair := topologyPair{key: c.topologyKey, value: topologyValue}
		currentCount := state.topologyPairToPodCounts[pair]

		// Find minimum count in all domains
		minCount := math.MaxInt32
		for tp, count := range state.topologyPairToPodCounts {
			if tp.key == c.topologyKey && count < minCount {
				minCount = count
			}
		}
		if minCount == math.MaxInt32 {
			minCount = 0
		}

		// Check if adding pod to this node would exceed maxSkew
		newCount := currentCount + 1
		skew := int32(newCount - minCount)
		if skew > c.maxSkew {
			return utils.NewStatus(utils.Unschedulable,
				fmt.Sprintf("topology spread constraint not satisfied: skew %d > maxSkew %d for key %s",
					skew, c.maxSkew, c.topologyKey))
		}
	}

	return utils.NewStatus(utils.Success, "")
}

// PreScore prepares data for scoring
func (p *PodTopologySpread) PreScore(ctx context.Context, pod *v1.Pod, nodes []*v1.Node) *utils.Status {
	return utils.NewStatus(utils.Success, "")
}

// Score gives a higher score to nodes that would result in more even distribution
func (p *PodTopologySpread) Score(ctx context.Context, pod *v1.Pod, nodeName string) (int64, *utils.Status) {
	if p.cache == nil {
		return 0, utils.NewStatus(utils.Success, "")
	}

	constraints := pod.Spec.TopologySpreadConstraints
	if len(constraints) == 0 {
		return 0, utils.NewStatus(utils.Success, "")
	}

	nodeInfo := p.cache.Nodes()[nodeName]
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return 0, utils.NewStatus(utils.Error, "node not found in cache")
	}

	node := nodeInfo.Node()

	podKey, _ := utils.GetPodKey(pod)
	p.preFilterStateLock.RLock()
	state := p.preFilterState[podKey]
	p.preFilterStateLock.RUnlock()

	if state == nil {
		return 0, utils.NewStatus(utils.Success, "")
	}

	var totalScore int64 = 0
	numConstraints := 0

	for _, c := range state.constraints {
		// Only consider ScheduleAnyway constraints for scoring
		if c.whenUnsatisfiable != v1.ScheduleAnyway {
			continue
		}

		topologyValue, exists := node.Labels[c.topologyKey]
		if !exists {
			continue
		}

		pair := topologyPair{key: c.topologyKey, value: topologyValue}
		currentCount := state.topologyPairToPodCounts[pair]

		// Find max and min count in all domains
		maxCount := 0
		minCount := math.MaxInt32
		for tp, count := range state.topologyPairToPodCounts {
			if tp.key == c.topologyKey {
				if count > maxCount {
					maxCount = count
				}
				if count < minCount {
					minCount = count
				}
			}
		}
		if minCount == math.MaxInt32 {
			minCount = 0
		}

		// Prefer nodes with fewer pods (more room for spreading)
		// Score inversely proportional to current count
		if maxCount > 0 {
			score := int64(100 * (maxCount - currentCount) / maxCount)
			totalScore += score
		} else {
			totalScore += 100
		}
		numConstraints++
	}

	if numConstraints == 0 {
		return 0, utils.NewStatus(utils.Success, "")
	}

	// Average score across all constraints
	finalScore := totalScore / int64(numConstraints)
	return finalScore, utils.NewStatus(utils.Success, "")
}

func (p *PodTopologySpread) ScoreExtensions() framework.ScoreExtensions {
	return p
}

func (p *PodTopologySpread) NormalizeScore(ctx context.Context, pod *v1.Pod, scores utils.PluginResult) *utils.Status {
	return utils.NewStatus(utils.Success, "")
}
