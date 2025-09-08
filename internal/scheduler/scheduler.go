package scheduler

import (
	"context"
	"fmt"
	logger "keti/ai-storage-scheduler/internal/backend/log"
	internalqueue "keti/ai-storage-scheduler/internal/backend/queue"
	config "keti/ai-storage-scheduler/internal/config"
	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"
	"sync"

	"k8s.io/apimachinery/pkg/util/wait"
)

var MainScheduler *Scheduler

var ErrNoNodesAvailable = fmt.Errorf("no nodes available to schedule pods")

type Scheduler struct {
	schedulerConfig *config.SchedulerConfig
	Cache           *utils.Cache
	NextPod         func() (*internalqueue.QueuedPodInfo, error)
	StopEverything  <-chan struct{}
	SchedulingQueue *internalqueue.SchedulingQueue
	fwk             framework.Framework
	logger          *logger.Logger
}

type ScheduleResult struct {
	SuggestedHost   string          // Name of the selected node.
	FeasibleNodes   int             // The number of nodes out of the evaluated ones that fit the pod.
	nominatingInfo  *NominatingInfo // The nominating info for scheduling cycle.
	PluginResultMap utils.PluginResultMap
}

func NewScheduleResult(nodeInfoMap map[string]*utils.NodeInfo) ScheduleResult {
	pluginResultMap := make(utils.PluginResultMap, len(nodeInfoMap))

	for nodeName, nodeInfo := range nodeInfoMap {
		gpuScores := make(map[string]*utils.GPUScore)

		if nodeInfo != nil && nodeInfo.GPUMap != nil {
			for gpuID := range nodeInfo.GPUMap {
				gpuScores[gpuID] = &utils.GPUScore{}
			}
		}

		pluginResultMap[nodeName] = utils.PluginResult{
			GPUScores:      gpuScores,
			IsFiltered:     false,
			Scores:         make([]utils.PluginScore, 0),
			TotalNodeScore: 0,
		}
	}

	return ScheduleResult{
		SuggestedHost:   "",
		FeasibleNodes:   0,
		nominatingInfo:  nil,
		PluginResultMap: utils.PluginResultMap{},
	}
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

func NewScheduler(ctx context.Context, cc *config.SchedulerConfig) (*Scheduler, error) {
	stopEverything := ctx.Done()
	schedulerCache := utils.NewCache(ctx)
	podQueue := internalqueue.NewSchedulingQueue(internalqueue.Less, cc.InformerFactory)
	logger := logger.NewLogger(logger.NewDefaultConfig())

	sched := &Scheduler{
		schedulerConfig: cc,
		Cache:           schedulerCache,
		StopEverything:  stopEverything,
		SchedulingQueue: podQueue,
		logger:          logger,
	}

	if err := AddAllEventHandlers(sched, cc.InformerFactory); err != nil {
		return nil, fmt.Errorf("adding event handlers: %w", err)
	}

	sched.NextPod = podQueue.Pop
	// sched.applyDefaultHandlers()

	return sched, nil
}

func (sched *Scheduler) InitScheduler() error {
	sched.schedulerConfig.InformerFactory.WaitForCacheSync(sched.StopEverything)

	return nil
}

func (sched *Scheduler) Run(ctx context.Context) {
	var internalWG sync.WaitGroup

	internalWG.Add(1)
	go func() {
		defer internalWG.Done()
		sched.schedulerConfig.InformerFactory.Start(ctx.Done())
	}()

	internalWG.Add(1)
	go func() {
		defer internalWG.Done()
		sched.SchedulingQueue.Run()
	}()

	internalWG.Add(1)
	go func() {
		defer internalWG.Done()
		wait.UntilWithContext(ctx, sched.ScheduleOne, 0)
	}()

	<-ctx.Done()
	sched.SchedulingQueue.Close()

	internalWG.Wait()
	logger.Info("All scheduler workers stopped")
}
