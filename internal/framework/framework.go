package framework

import (
	"context"
	"fmt"

	logger "keti/ai-storage-scheduler/internal/backend/log"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// PluginWeight represents a plugin with its weight for scoring
type PluginWeight struct {
	Plugin ScorePlugin
	Weight int32
}

// frameworkImpl implements the Framework interface
type frameworkImpl struct {
	preFilterPlugins  []PreFilterPlugin
	filterPlugins     []FilterPlugin
	postFilterPlugins []PostFilterPlugin
	preScorePlugins   []PreScorePlugin
	scorePlugins      []PluginWeight
	reservePlugins    []ReservePlugin
	preBindPlugins    []PreBindPlugin
	bindPlugin        BindPlugin
	postBindPlugins   []PostBindPlugin
}

// FrameworkConfig holds the configuration for creating a Framework
type FrameworkConfig struct {
	PreFilterPlugins  []PreFilterPlugin
	FilterPlugins     []FilterPlugin
	PostFilterPlugins []PostFilterPlugin
	PreScorePlugins   []PreScorePlugin
	ScorePlugins      []PluginWeight
	ReservePlugins    []ReservePlugin
	PreBindPlugins    []PreBindPlugin
	BindPlugin        BindPlugin
	PostBindPlugins   []PostBindPlugin
}

// NewFramework creates a new Framework instance (legacy constructor for backward compatibility)
func NewFramework(filterPlugins []FilterPlugin, scorePlugins []ScorePlugin, bindPlugin BindPlugin) Framework {
	weightedScorePlugins := make([]PluginWeight, len(scorePlugins))
	for i, p := range scorePlugins {
		weightedScorePlugins[i] = PluginWeight{Plugin: p, Weight: 1}
	}
	return &frameworkImpl{
		filterPlugins: filterPlugins,
		scorePlugins:  weightedScorePlugins,
		bindPlugin:    bindPlugin,
	}
}

// NewFrameworkWithConfig creates a new Framework with full configuration
func NewFrameworkWithConfig(config *FrameworkConfig) Framework {
	return &frameworkImpl{
		preFilterPlugins:  config.PreFilterPlugins,
		filterPlugins:     config.FilterPlugins,
		postFilterPlugins: config.PostFilterPlugins,
		preScorePlugins:   config.PreScorePlugins,
		scorePlugins:      config.ScorePlugins,
		reservePlugins:    config.ReservePlugins,
		preBindPlugins:    config.PreBindPlugins,
		bindPlugin:        config.BindPlugin,
		postBindPlugins:   config.PostBindPlugins,
	}
}

// RunPreFilterPlugins runs all PreFilter plugins
func (f *frameworkImpl) RunPreFilterPlugins(ctx context.Context, pod *v1.Pod) (*PreFilterResult, *utils.Status) {
	var result *PreFilterResult

	for _, plugin := range f.preFilterPlugins {
		r, status := plugin.PreFilter(ctx, pod)
		if !status.IsSuccess() {
			logger.Info("[prefilter-plugin] PreFilter failed",
				"namespace", pod.Namespace, "pod", pod.Name,
				"plugin", plugin.Name(),
				"reason", status.Message())
			return nil, status
		}

		logger.Info("[prefilter-plugin] PreFilter passed",
			"namespace", pod.Namespace, "pod", pod.Name,
			"plugin", plugin.Name())

		// Merge node names if specified
		if r != nil && r.NodeNames != nil {
			if result == nil {
				result = &PreFilterResult{NodeNames: make(map[string]struct{})}
				for k, v := range r.NodeNames {
					result.NodeNames[k] = v
				}
			} else {
				// Intersect node names
				for k := range result.NodeNames {
					if _, ok := r.NodeNames[k]; !ok {
						delete(result.NodeNames, k)
					}
				}
			}
		}
	}

	return result, utils.NewStatus(utils.Success, "")
}

func (f *frameworkImpl) RunFilterPlugins(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) utils.PluginResultMap {
	result := utils.PluginResultMap{}

	if nodeInfo == nil || nodeInfo.Node() == nil {
		return result
	}

	nodeName := nodeInfo.Node().Name

	// Initialize result for this node
	result[nodeName] = utils.PluginResult{
		IsFiltered:   false,
		Scores:       []utils.PluginScore{},
		FilterReason: "",
	}

	// Run all filter plugins
	for _, plugin := range f.filterPlugins {
		status := plugin.Filter(ctx, pod, nodeInfo)

		if !status.IsSuccess() {
			logger.Info("[filter-plugin] Node filtered out",
				"namespace", pod.Namespace, "pod", pod.Name,
				"node", nodeName,
				"plugin", plugin.Name(),
				"reason", status.Message())

			// Mark node as filtered out
			pluginResult := result[nodeName]
			pluginResult.IsFiltered = true
			pluginResult.FilterReason = fmt.Sprintf("%s: %s", plugin.Name(), status.Message())
			result[nodeName] = pluginResult
			return result
		} else {
			logger.Info("[filter-plugin] Node passed filter",
				"namespace", pod.Namespace, "pod", pod.Name,
				"node", nodeName,
				"plugin", plugin.Name())
		}
	}

	logger.Info("[filter-plugin] Node passed all filters",
		"namespace", pod.Namespace, "pod", pod.Name,
		"node", nodeName)

	return result
}

// RunPostFilterPlugins runs all PostFilter plugins (for preemption)
func (f *frameworkImpl) RunPostFilterPlugins(ctx context.Context, pod *v1.Pod, filteredNodeStatusMap map[string]*utils.Status) (*PostFilterResult, *utils.Status) {
	for _, plugin := range f.postFilterPlugins {
		result, status := plugin.PostFilter(ctx, pod, filteredNodeStatusMap)
		if status.IsSuccess() && result != nil && result.NominatedNodeName != "" {
			logger.Info("[postfilter-plugin] Found candidate node for preemption",
				"namespace", pod.Namespace, "pod", pod.Name,
				"plugin", plugin.Name(),
				"nominated_node", result.NominatedNodeName)
			return result, status
		}
	}
	return nil, utils.NewStatus(utils.Unschedulable, "no preemption candidate found")
}

// RunPreScorePlugins runs all PreScore plugins
func (f *frameworkImpl) RunPreScorePlugins(ctx context.Context, pod *v1.Pod, nodes []*v1.Node) *utils.Status {
	for _, plugin := range f.preScorePlugins {
		status := plugin.PreScore(ctx, pod, nodes)
		if !status.IsSuccess() {
			logger.Error("[prescore-plugin] PreScore failed",
				fmt.Errorf("%s", status.Message()),
				"namespace", pod.Namespace, "pod", pod.Name,
				"plugin", plugin.Name())
			return status
		}
	}
	return utils.NewStatus(utils.Success, "")
}

func (f *frameworkImpl) RunScorePlugins(ctx context.Context, pod *v1.Pod, nodes []*v1.Node) (utils.PluginResultMap, *utils.Status) {
	result := utils.PluginResultMap{}

	// Initialize results
	for _, node := range nodes {
		result[node.Name] = utils.PluginResult{
			IsFiltered:     false,
			Scores:         make([]utils.PluginScore, 0),
			TotalNodeScore: 0,
		}
	}

	// Run all score plugins
	for _, pw := range f.scorePlugins {
		plugin := pw.Plugin
		weight := pw.Weight
		if weight == 0 {
			weight = 1
		}

		logger.Info("[score-plugin] Running score plugin",
			"namespace", pod.Namespace, "pod", pod.Name,
			"plugin", plugin.Name(),
			"weight", weight,
			"node_count", len(nodes))

		for _, node := range nodes {
			score, status := plugin.Score(ctx, pod, node.Name)
			if !status.IsSuccess() {
				logger.Error("[score-plugin] Scoring failed",
					fmt.Errorf("%s", status.Message()),
					"namespace", pod.Namespace, "pod", pod.Name,
					"plugin", plugin.Name(),
					"node", node.Name)
				return nil, status
			}

			// Apply weight to score
			weightedScore := score * int64(weight)

			pluginResult := result[node.Name]
			pluginResult.Scores = append(pluginResult.Scores, utils.PluginScore{
				PluginName: plugin.Name(),
				Score:      weightedScore,
			})
			pluginResult.TotalNodeScore += int(weightedScore)
			result[node.Name] = pluginResult

			logger.Info("[score-plugin] Node scored",
				"namespace", pod.Namespace, "pod", pod.Name,
				"plugin", plugin.Name(),
				"node", node.Name,
				"raw_score", score,
				"weight", weight,
				"weighted_score", weightedScore)
		}
	}

	return result, utils.NewStatus(utils.Success, "")
}

// RunReservePlugins runs all Reserve plugins
func (f *frameworkImpl) RunReservePlugins(ctx context.Context, pod *v1.Pod, nodeName string) *utils.Status {
	for _, plugin := range f.reservePlugins {
		status := plugin.Reserve(ctx, pod, nodeName)
		if !status.IsSuccess() {
			logger.Error("[reserve-plugin] Reserve failed",
				fmt.Errorf("%s", status.Message()),
				"namespace", pod.Namespace, "pod", pod.Name,
				"plugin", plugin.Name(),
				"node", nodeName)
			// Unreserve previously reserved plugins
			f.RunUnreservePlugins(ctx, pod, nodeName)
			return status
		}
	}
	return utils.NewStatus(utils.Success, "")
}

// RunUnreservePlugins runs all Unreserve methods
func (f *frameworkImpl) RunUnreservePlugins(ctx context.Context, pod *v1.Pod, nodeName string) {
	for _, plugin := range f.reservePlugins {
		plugin.Unreserve(ctx, pod, nodeName)
	}
}

// RunPreBindPlugins runs all PreBind plugins
func (f *frameworkImpl) RunPreBindPlugins(ctx context.Context, pod *v1.Pod, nodeName string) *utils.Status {
	for _, plugin := range f.preBindPlugins {
		status := plugin.PreBind(ctx, pod, nodeName)
		if !status.IsSuccess() {
			logger.Error("[prebind-plugin] PreBind failed",
				fmt.Errorf("%s", status.Message()),
				"namespace", pod.Namespace, "pod", pod.Name,
				"plugin", plugin.Name(),
				"node", nodeName)
			return status
		}
	}
	return utils.NewStatus(utils.Success, "")
}

func (f *frameworkImpl) RunBindPlugin(ctx context.Context, pod *v1.Pod, nodeName string) *utils.Status {
	if f.bindPlugin == nil {
		return utils.NewStatus(utils.Error, "no bind plugin configured")
	}

	logger.Info("[bind-plugin] Starting bind",
		"namespace", pod.Namespace, "pod", pod.Name,
		"node", nodeName,
		"plugin", f.bindPlugin.Name())

	status := f.bindPlugin.Bind(ctx, pod, nodeName)
	if !status.IsSuccess() {
		klog.ErrorS(fmt.Errorf("%s", status.Message()), "Failed to bind pod",
			"pod", klog.KObj(pod),
			"node", nodeName)
		return status
	}

	logger.Info("[bind-plugin] Successfully bound pod to node",
		"namespace", pod.Namespace, "pod", pod.Name,
		"node", nodeName)

	return utils.NewStatus(utils.Success, "")
}

// RunPostBindPlugins runs all PostBind plugins
func (f *frameworkImpl) RunPostBindPlugins(ctx context.Context, pod *v1.Pod, nodeName string) {
	for _, plugin := range f.postBindPlugins {
		plugin.PostBind(ctx, pod, nodeName)
	}
}
