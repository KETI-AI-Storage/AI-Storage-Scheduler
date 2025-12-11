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
	schedulerCache := cc.Cache
	podQueue := internalqueue.NewSchedulingQueue(internalqueue.Less, cc.InformerFactory)
	logger := logger.NewLogger(logger.NewDefaultConfig())

	// Initialize framework with plugins
	fwk := cc.Framework
	if fwk == nil {
		return nil, fmt.Errorf("framework is required")
	}

	sched := &Scheduler{
		schedulerConfig: cc,
		Cache:           schedulerCache,
		StopEverything:  stopEverything,
		SchedulingQueue: podQueue,
		fwk:             fwk,
		logger:          logger,
	}

	if err := AddAllEventHandlers(sched, cc.InformerFactory); err != nil {
		return nil, fmt.Errorf("adding event handlers: %w", err)
	}

	sched.NextPod = podQueue.Pop

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

	// GPU metrics worker
	internalWG.Add(1)
	go func() {
		defer internalWG.Done()
		sched.gpuMetricsWorker(ctx)
	}()

	<-ctx.Done()
	sched.SchedulingQueue.Close()

	internalWG.Wait()
	logger.Info("All scheduler workers stopped")
}

// GPU Metrics Collection Functions

// fetchNodeGPUMetrics fetches GPU metrics for a specific node
// TODO: Implement actual gRPC call to fetch GPU metrics
func (sched *Scheduler) fetchNodeGPUMetrics(nodeName string) {
	logger.Info(fmt.Sprintf("[gpu-metrics] Fetching GPU metrics for node: %s", nodeName))

	// TODO: Call gRPC service to get GPU metrics
	// Example:
	// gpuMetrics, err := grpc.GetNodeGPUMetrics(nodeName)
	// if err != nil {
	//     logger.Error("Failed to fetch GPU metrics", err)
	//     return
	// }

	// Update cache with GPU metrics
	sched.Cache.UpdateNodeGPUMetrics(nodeName, nil) // nil = blank implementation
}

// refreshStaleGPUMetrics refreshes GPU metrics for nodes where metrics are older than maxAge
func (sched *Scheduler) refreshStaleGPUMetrics(ctx context.Context, maxAge int64) {
	nodes := sched.Cache.Nodes()
	currentTime := utils.GetCurrentTimeMillis()

	for nodeName, nodeInfo := range nodes {
		if nodeInfo == nil {
			continue
		}

		// Check if GPU metrics are stale
		metricsAge := currentTime - nodeInfo.GPUMetricsUpdatedAt.UnixMilli()
		if metricsAge > maxAge {
			logger.Info(fmt.Sprintf("[gpu-metrics] Refreshing stale GPU metrics for node: %s (age: %dms)", nodeName, metricsAge))
			sched.fetchNodeGPUMetrics(nodeName)
		}
	}
}

// refreshAllGPUMetrics refreshes GPU metrics for all nodes
func (sched *Scheduler) refreshAllGPUMetrics() {
	logger.Info("[gpu-metrics] Refreshing GPU metrics for all nodes")

	nodes := sched.Cache.Nodes()
	for nodeName := range nodes {
		sched.fetchNodeGPUMetrics(nodeName)
	}
}

// gpuMetricsWorker periodically refreshes GPU metrics for all nodes
func (sched *Scheduler) gpuMetricsWorker(ctx context.Context) {
	logger.Info("[gpu-metrics] Starting GPU metrics worker")

	// Initial collection
	sched.refreshAllGPUMetrics()

	// Periodic refresh every 5 minutes
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		sched.refreshAllGPUMetrics()
	}, 300000000000) // 5 minutes in nanoseconds

	logger.Info("[gpu-metrics] GPU metrics worker stopped")
}
