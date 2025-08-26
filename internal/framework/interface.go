package framework

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
)

type Framework interface {
	QueueSortFunc() LessFunc
	RunPreFilterPlugins(ctx context.Context, state *CycleState, pod *v1.Pod) (*PreFilterResult, *Status, sets.Set[string])
	RunPostFilterPlugins(ctx context.Context, state *CycleState, pod *v1.Pod, filteredNodeStatusMap Node)
	RunPostBindPlugins(ctx context.Context, state *CycleState, pod *v1.Pod, nodeName string)
	RunReservePluginsReserve(ctx context.Context, state *CycleState, pod *v1.Pod, nodeName string) *Status
	RunReservePluginsUnreserve(ctx context.Context, state *CycleState, pod *v1.Pod, nodeName string)
	RunBindPlugins(ctx context.Context, state *CycleState, pod *v1.Pod, nodeName string) *Status
	HasFilterPlugins() bool
	HasPostFilterPlugins() bool
	HasScorePlugins() bool
	ListPlugins() *config.Plugins
	ProfileName() string
	PercentageOfNodesToScore() *int32
	SetPodNominator(nominator PodNominator)
	SetPodActivator(activator PodActivator)
	Close() error
}

type NominatingMode int

const (
	ModeNoop NominatingMode = iota
	ModeOverride
)

type NominatingInfo struct {
	NominatedNodeName string
	NominatingMode    NominatingMode
}

func (ni *NominatingInfo) Mode() NominatingMode {
	if ni == nil {
		return ModeNoop
	}
	return ni.NominatingMode
}

type LessFunc func(podInfo1, podInfo2 *QueuedPodInfo) bool
