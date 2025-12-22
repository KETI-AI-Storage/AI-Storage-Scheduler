package framework

import (
	"context"

	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
)

// Plugin is the base interface for all scheduling plugins.
type Plugin interface {
	Name() string
}

// PreFilterPlugin is an interface for plugins that need to pre-process information
// about a Pod before the scheduling cycle begins.
type PreFilterPlugin interface {
	Plugin
	// PreFilter is called before the scheduling cycle begins.
	// It returns a PreFilterResult and a Status.
	PreFilter(ctx context.Context, pod *v1.Pod) (*PreFilterResult, *utils.Status)
	// PreFilterExtensions returns a PreFilterExtensions interface if the plugin implements one.
	PreFilterExtensions() PreFilterExtensions
}

// PreFilterResult contains the result of PreFilter phase.
type PreFilterResult struct {
	// NodeNames is a set of node names that should be considered.
	// nil means all nodes should be considered.
	NodeNames map[string]struct{}
}

// PreFilterExtensions is an interface for "AddPod"/"RemovePod" methods.
type PreFilterExtensions interface {
	// AddPod is called when a pod is added to the cluster during scheduling.
	AddPod(ctx context.Context, podToSchedule *v1.Pod, podInfoToAdd *utils.PodInfo, nodeInfo *utils.NodeInfo) *utils.Status
	// RemovePod is called when a pod is removed from the cluster during scheduling.
	RemovePod(ctx context.Context, podToSchedule *v1.Pod, podInfoToRemove *utils.PodInfo, nodeInfo *utils.NodeInfo) *utils.Status
}

// FilterPlugin is an interface for Filter plugins.
type FilterPlugin interface {
	Plugin
	// Filter is called to filter out nodes that cannot run the pod.
	Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status
}

// PostFilterPlugin is an interface for PostFilter plugins (used for preemption).
type PostFilterPlugin interface {
	Plugin
	// PostFilter is called when no nodes passed the Filter phase.
	PostFilter(ctx context.Context, pod *v1.Pod, filteredNodeStatusMap map[string]*utils.Status) (*PostFilterResult, *utils.Status)
}

// PostFilterResult contains the result of PostFilter phase.
type PostFilterResult struct {
	NominatedNodeName string
}

// PreScorePlugin is an interface for PreScore plugins.
type PreScorePlugin interface {
	Plugin
	// PreScore is called before scoring.
	PreScore(ctx context.Context, pod *v1.Pod, nodes []*v1.Node) *utils.Status
}

// ScorePlugin is an interface for Score plugins.
type ScorePlugin interface {
	Plugin
	// Score is called for each node that passed filtering.
	Score(ctx context.Context, pod *v1.Pod, nodeName string) (int64, *utils.Status)
	// ScoreExtensions returns a ScoreExtensions interface.
	ScoreExtensions() ScoreExtensions
}

// ScoreExtensions is an interface for normalizing scores.
type ScoreExtensions interface {
	// NormalizeScore is called after all nodes have been scored.
	NormalizeScore(ctx context.Context, pod *v1.Pod, scores utils.PluginResult) *utils.Status
}

// ReservePlugin is an interface for Reserve plugins.
type ReservePlugin interface {
	Plugin
	// Reserve is called when the scheduler assumes a pod.
	Reserve(ctx context.Context, pod *v1.Pod, nodeName string) *utils.Status
	// Unreserve is called when Reserve fails or the pod is rejected.
	Unreserve(ctx context.Context, pod *v1.Pod, nodeName string)
}

// PreBindPlugin is an interface for PreBind plugins.
type PreBindPlugin interface {
	Plugin
	// PreBind is called before binding.
	PreBind(ctx context.Context, pod *v1.Pod, nodeName string) *utils.Status
}

// BindPlugin is an interface for Bind plugins.
type BindPlugin interface {
	Plugin
	// Bind binds a pod to a node.
	Bind(ctx context.Context, pod *v1.Pod, nodeName string) *utils.Status
}

// PostBindPlugin is an interface for PostBind plugins.
type PostBindPlugin interface {
	Plugin
	// PostBind is called after binding.
	PostBind(ctx context.Context, pod *v1.Pod, nodeName string)
}

// Framework is the interface for the scheduling framework.
type Framework interface {
	// RunPreFilterPlugins runs the PreFilter plugins.
	RunPreFilterPlugins(ctx context.Context, pod *v1.Pod) (*PreFilterResult, *utils.Status)
	// RunFilterPlugins runs the Filter plugins.
	RunFilterPlugins(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) utils.PluginResultMap
	// RunPostFilterPlugins runs the PostFilter plugins.
	RunPostFilterPlugins(ctx context.Context, pod *v1.Pod, filteredNodeStatusMap map[string]*utils.Status) (*PostFilterResult, *utils.Status)
	// RunPreScorePlugins runs the PreScore plugins.
	RunPreScorePlugins(ctx context.Context, pod *v1.Pod, nodes []*v1.Node) *utils.Status
	// RunScorePlugins runs the Score plugins.
	RunScorePlugins(ctx context.Context, pod *v1.Pod, nodes []*v1.Node) (utils.PluginResultMap, *utils.Status)
	// RunReservePlugins runs the Reserve plugins.
	RunReservePlugins(ctx context.Context, pod *v1.Pod, nodeName string) *utils.Status
	// RunUnreservePlugins runs the Unreserve methods.
	RunUnreservePlugins(ctx context.Context, pod *v1.Pod, nodeName string)
	// RunPreBindPlugins runs the PreBind plugins.
	RunPreBindPlugins(ctx context.Context, pod *v1.Pod, nodeName string) *utils.Status
	// RunBindPlugin runs the Bind plugin.
	RunBindPlugin(ctx context.Context, pod *v1.Pod, nodeName string) *utils.Status
	// RunPostBindPlugins runs the PostBind plugins.
	RunPostBindPlugins(ctx context.Context, pod *v1.Pod, nodeName string)
}
