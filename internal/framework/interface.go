package framework

import (
	"context"

	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
)

type Plugin interface {
	Name() string
}

type FilterPlugin interface {
	Plugin
	Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status
}

type ScorePlugin interface {
	Plugin
	Score(ctx context.Context, pod *v1.Pod, nodeName string) (int64, *utils.Status)
	ScoreExtensions() ScoreExtensions
}

type ScoreExtensions interface {
	NormalizeScore(ctx context.Context, pod *v1.Pod, scores utils.PluginResult) *utils.Status
}

type BindPlugin interface {
	Plugin
	Bind(ctx context.Context, pod *v1.Pod, nodeName string) *utils.Status
}

type Framework interface {
	RunFilterPlugins(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) utils.PluginResultMap
	RunScorePlugins(ctx context.Context, pod *v1.Pod, nodes []*v1.Node) (utils.PluginResultMap, *utils.Status)
	RunBindPlugin(ctx context.Context, pod *v1.Pod, nodeName string) *utils.Status
}
