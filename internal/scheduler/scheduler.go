package scheduler

import (
	"context"
	"fmt"
	cache "keti/ai-storage-scheduler/internal/backend/cache"
	internalqueue "keti/ai-storage-scheduler/internal/backend/queue"
	config "keti/ai-storage-scheduler/internal/config"
	framework "keti/ai-storage-scheduler/internal/framework"
	logger "keti/ai-storage-scheduler/internal/log"
	profile "keti/ai-storage-scheduler/internal/profile"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

var MainScheduler *Scheduler

type Scheduler struct {
	schedulerConfig    *config.SchedulerConfig
	nextStartNodeIndex int
	Cache              *cache.Cache
	NextPod            func(logger klog.Logger) (*framework.QueuedPodInfo, error)
	FailureHandler     FailureHandlerFn
	SchedulePod        func(ctx context.Context, fwk framework.Framework, state *framework.CycleState, pod *v1.Pod) (ScheduleResult, error)
	StopEverything     <-chan struct{}
	SchedulingQueue    internalqueue.PriorityQueue
	Profiles           profile.Map
	logger             klog.Logger
}

type ScheduleResult struct {
	SuggestedHost  string                    // Name of the selected node.
	EvaluatedNodes int                       // The number of nodes the scheduler evaluated the pod against in the filtering phase.
	FeasibleNodes  int                       // The number of nodes out of the evaluated ones that fit the pod.
	nominatingInfo *framework.NominatingInfo // The nominating info for scheduling cycle.
}

type FailureHandlerFn func(ctx context.Context, fwk framework.Framework, podInfo *framework.QueuedPodInfo, status *framework.Status, nominatingInfo *framework.NominatingInfo, start time.Time)

func NewScheduler(ctx context.Context, cc *config.SchedulerConfig) (*Scheduler, error) {
	stopEverything := ctx.Done()

	schedulerCache := cache.New(ctx)

	podLister := cc.InformerFactory.Core().V1().Pods().Lister()

	podQueue := internalqueue.NewSchedulingQueue()

	sched := &Scheduler{
		schedulerConfig: cc,
		Cache:           schedulerCache,
		StopEverything:  stopEverything,
		SchedulingQueue: podQueue,
		Profiles:        profiles,
		logger:          logger,
	}

	if err := AddAllEventHandlers(sched, cc.InformerFactory); err != nil {
		return nil, fmt.Errorf("adding event handlers: %w", err)
	}

	return sched, nil
}

func (sched *Scheduler) InitScheduler() error {
	return nil
}

func (sched *Scheduler) Run(quitChan <-chan struct{}) {
	var internalWG sync.WaitGroup

	internalWG.Add(1)
	go func() {
		defer internalWG.Done()
		sched.InformerFactory.Start(quitChan)
	}()

	internalWG.Add(1)
	go func() {
		defer internalWG.Done()
		sched.ScheduleOne(quitChan)
	}()

	internalWG.Wait()
	logger.Info("All scheduler workers stopped")
}
