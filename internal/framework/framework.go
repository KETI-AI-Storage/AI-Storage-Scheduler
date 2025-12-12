package framework

import (
	"context"
	"fmt"

	logger "keti/ai-storage-scheduler/internal/backend/log"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// frameworkImpl implements the Framework interface
type frameworkImpl struct {
	filterPlugins []FilterPlugin
	scorePlugins  []ScorePlugin
	bindPlugin    BindPlugin
}

// NewFramework creates a new Framework instance
func NewFramework(filterPlugins []FilterPlugin, scorePlugins []ScorePlugin, bindPlugin BindPlugin) Framework {
	return &frameworkImpl{
		filterPlugins: filterPlugins,
		scorePlugins:  scorePlugins,
		bindPlugin:    bindPlugin,
	}
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
			logger.Debug("[filter-plugin] Node passed filter",
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
	for _, plugin := range f.scorePlugins {
		logger.Info("[score-plugin] Running score plugin",
			"namespace", pod.Namespace, "pod", pod.Name,
			"plugin", plugin.Name(),
			"node_count", len(nodes))
		
		for _, node := range nodes {
			score, status := plugin.Score(ctx, pod, node.Name)
			if !status.IsSuccess() {
				logger.Error("[score-plugin] Scoring failed", 
					fmt.Errorf(status.Message()),
					"namespace", pod.Namespace, "pod", pod.Name,
					"plugin", plugin.Name(),
					"node", node.Name)
				return nil, status
			}

			pluginResult := result[node.Name]
			pluginResult.Scores = append(pluginResult.Scores, utils.PluginScore{
				PluginName: plugin.Name(),
				Score:      score,
			})
			pluginResult.TotalNodeScore += int(score)
			result[node.Name] = pluginResult

			logger.Info("[score-plugin] Node scored",
				"namespace", pod.Namespace, "pod", pod.Name,
				"plugin", plugin.Name(),
				"node", node.Name,
				"score", score)
		}
	}

	return result, utils.NewStatus(utils.Success, "")
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
		klog.ErrorS(fmt.Errorf(status.Message()), "Failed to bind pod",
			"pod", klog.KObj(pod),
			"node", nodeName)
		return status
	}

	logger.Info("[bind-plugin] Successfully bound pod to node",
		"namespace", pod.Namespace, "pod", pod.Name,
		"node", nodeName)

	return utils.NewStatus(utils.Success, "")
}
