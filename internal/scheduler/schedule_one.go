package scheduler

import (
	"context"
	"fmt"
	"time"

	internalqueue "keti/ai-storage-scheduler/internal/backend/queue"
	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

func (sched *Scheduler) ScheduleOne(ctx context.Context) {
	podInfo, err := sched.NextPod()

	if err != nil {
		// logger.Error(err, "Error while retrieving next pod from scheduling queue")
		return
	}

	if podInfo == nil || podInfo.Pod == nil {
		return // when schedulerQueue is closed
	}

	pod := podInfo.Pod
	// logger = klog.LoggerWithValues(logger, "pod", klog.KObj(pod))
	// ctx = klog.NewContext(ctx, logger)
	// logger.V(4).Info("About to try and schedule pod", "pod", klog.KObj(pod))

	fwk, err := sched.frameworkForPod(pod)
	if err != nil {
		sched.SchedulingQueue.Done(pod.UID)
		return
	}

	// logger.V(3).Info("Attempting to schedule pod", "pod", klog.KObj(pod))

	start := time.Now()

	schedulingCycleCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	scheduleResult, assumedPodInfo, status := sched.schedulingCycle(schedulingCycleCtx, fwk, podInfo, start)
	if !status.IsSuccess() {
		// sched.FailureHandler(schedulingCycleCtx, fwk, assumedPodInfo, status, scheduleResult.nominatingInfo, start)
		return
	}

	// bind the pod to its host asynchronously (we can do this b/c of the assumption step above).
	go func() {
		bindingCycleCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		status := sched.bindingCycle(bindingCycleCtx, fwk, scheduleResult, assumedPodInfo, start)
		if !status.IsSuccess() {
			// sched.handleBindingCycleError(bindingCycleCtx, state, fwk, assumedPodInfo, start, scheduleResult, status)
			return
		}
	}()
}

func (sched *Scheduler) frameworkForPod(pod *v1.Pod) (framework.Framework, error) {
	// For now, return the default framework
	// In the future, you can select different frameworks based on pod characteristics
	return sched.fwk, nil
}

var clearNominatedNode = &NominatingInfo{NominatingMode: ModeOverride, NominatedNodeName: ""}

// schedulingCycle tries to schedule a single Pod.
func (sched *Scheduler) schedulingCycle(
	ctx context.Context,
	fwk framework.Framework,
	podInfo *internalqueue.QueuedPodInfo,
	start time.Time) (ScheduleResult, *internalqueue.QueuedPodInfo, *utils.Status) {

	logger := klog.FromContext(ctx)
	pod := podInfo.Pod

	scheduleResult, err := sched.schedulePod(ctx, fwk, pod)
	if err != nil {
		if err == ErrNoNodesAvailable {
			status := utils.NewStatus(utils.UnschedulableAndUnresolvable).WithError(err)
			return ScheduleResult{nominatingInfo: clearNominatedNode}, podInfo, status
		}

		// 각종 스케줄링 에러 처리

		// Run PostFilter plugins to attempt to make the pod schedulable in a future scheduling cycle.
		// RunPostFilterPlugins(ctx, state, pod, fitError.Diagnosis.NodeToStatus)
		var nominatingInfo *NominatingInfo
		// if result != nil {
		// 	nominatingInfo = result.NominatingInfo
		// }
		return ScheduleResult{nominatingInfo: nominatingInfo}, podInfo, utils.NewStatus(utils.Unschedulable).WithError(err)
	}

	assumedPodInfo := podInfo.DeepCopy()
	assumedPod := assumedPodInfo.Pod

	err = sched.assume(logger, assumedPod, scheduleResult.SuggestedHost)
	if err != nil {
		return ScheduleResult{nominatingInfo: clearNominatedNode}, assumedPodInfo, utils.AsStatus(err)
	}

	// RunReservePluginsReserve(ctx, state, assumedPod, scheduleResult.SuggestedHost);
	// RunPermitPlugins(ctx, state, assumedPod, scheduleResult.SuggestedHost)
	// At the end of a successful scheduling cycle, pop and move up Pods if needed. sched.SchedulingQueue.Activate(logger, podsToActivate.Map)

	return scheduleResult, assumedPodInfo, nil
}

func (sched *Scheduler) assume(logger klog.Logger, assumed *v1.Pod, host string) error {
	assumed.Spec.NodeName = host

	if err := sched.Cache.AssumePod(logger, assumed); err != nil {
		logger.Error(err, "Scheduler cache AssumePod failed")
		return err
	}
	// if "assumed" is a nominated pod, we should remove it from internal cache
	if sched.SchedulingQueue != nil {
		sched.SchedulingQueue.DeleteNominatedPodIfExists(assumed)
	}

	return nil
}

// bindingCycle tries to bind an assumed Pod.
func (sched *Scheduler) bindingCycle(
	ctx context.Context,
	fwk framework.Framework,
	scheduleResult ScheduleResult,
	assumedPodInfo *internalqueue.QueuedPodInfo,
	start time.Time) *utils.Status {

	assumedPod := assumedPodInfo.Pod

	// Run "permit" plugins.WaitOnPermit(ctx, assumedPod)
	// Run "prebind" plugins.RunPreBindPlugins(ctx, state, assumedPod, scheduleResult.SuggestedHost)

	err := sched.runBindPlugin(ctx, fwk, assumedPod, &scheduleResult)
	if err != nil {
		// 에러처리
	}

	sched.SchedulingQueue.Done(assumedPod.UID)

	return nil
}

func (sched *Scheduler) schedulePod(ctx context.Context, fwk framework.Framework, pod *v1.Pod) (result ScheduleResult, err error) {
	if sched.Cache.NodeCount() == 0 {
		return result, ErrNoNodesAvailable
	}

	// Refresh stale GPU metrics if pod requests GPU
	if hasGPURequest(pod) {
		// Refresh metrics older than 60 seconds (60000 milliseconds)
		sched.refreshStaleGPUMetrics(ctx, 60000)
	}

	nodes := sched.Cache.Nodes()
	if nodes == nil {
		// 에러처리
	}
	scheduleResult := NewScheduleResult(nodes)

	err = sched.runFilterPlugin(ctx, fwk, pod, &scheduleResult)
	if err != nil {
		return result, err
	}

	if scheduleResult.FeasibleNodes == 0 {
		// all node filtered
	} else if scheduleResult.FeasibleNodes == 1 {
		for name, pr := range scheduleResult.PluginResultMap {
			if pr.IsFiltered {
				scheduleResult.SuggestedHost = name
			}
		}
		return scheduleResult, nil
	}

	err = sched.runScorePlugin(ctx, fwk, pod, &scheduleResult)
	if err != nil {
		return result, err
	}

	err = sched.selectResource(pod, &scheduleResult)

	return scheduleResult, err
}

func (sched *Scheduler) runFilterPlugin(ctx context.Context, fwk framework.Framework, pod *v1.Pod, scheduleResult *ScheduleResult) error {
	nodes := sched.Cache.Nodes()
	feasibleNodes := 0

	for nodeName, nodeInfo := range nodes {
		pluginResults := fwk.RunFilterPlugins(ctx, pod, nodeInfo)

		if pr, exists := pluginResults[nodeName]; exists {
			existingResult := scheduleResult.PluginResultMap[nodeName]
			existingResult.IsFiltered = pr.IsFiltered
			scheduleResult.PluginResultMap[nodeName] = existingResult

			if !pr.IsFiltered {
				feasibleNodes++
			}
		}
	}

	scheduleResult.FeasibleNodes = feasibleNodes
	return nil
}

func (sched *Scheduler) runScorePlugin(ctx context.Context, fwk framework.Framework, pod *v1.Pod, scheduleResult *ScheduleResult) error {
	// Get list of feasible nodes
	feasibleNodes := []*v1.Node{}
	for nodeName, pr := range scheduleResult.PluginResultMap {
		if !pr.IsFiltered {
			if nodeInfo := sched.Cache.Nodes()[nodeName]; nodeInfo != nil && nodeInfo.Node() != nil {
				feasibleNodes = append(feasibleNodes, nodeInfo.Node())
			}
		}
	}

	if len(feasibleNodes) == 0 {
		return fmt.Errorf("no feasible nodes")
	}

	// Run score plugins
	scores, status := fwk.RunScorePlugins(ctx, pod, feasibleNodes)
	if !status.IsSuccess() {
		return fmt.Errorf("scoring failed: %s", status.Message())
	}

	// Merge scores into scheduleResult
	for nodeName, score := range scores {
		if existing, ok := scheduleResult.PluginResultMap[nodeName]; ok {
			existing.Scores = score.Scores
			existing.TotalNodeScore = score.TotalNodeScore
			scheduleResult.PluginResultMap[nodeName] = existing
		}
	}

	return nil
}

func (sched *Scheduler) selectResource(pod *v1.Pod, scheduleResult *ScheduleResult) error {
	// Select the node with the highest score
	var bestNode string
	var bestScore int = -1

	for nodeName, pr := range scheduleResult.PluginResultMap {
		if !pr.IsFiltered && pr.TotalNodeScore > bestScore {
			bestScore = pr.TotalNodeScore
			bestNode = nodeName
		}
	}

	if bestNode == "" {
		return fmt.Errorf("no suitable node found")
	}

	scheduleResult.SuggestedHost = bestNode
	return nil
}

func (sched *Scheduler) runBindPlugin(ctx context.Context, fwk framework.Framework, assumed *v1.Pod, scheduleResult *ScheduleResult) error {
	defer func() {
		sched.finishBinding(fwk, assumed, scheduleResult.SuggestedHost)
	}()

	// Run bind plugin
	status := fwk.RunBindPlugin(ctx, assumed, scheduleResult.SuggestedHost)
	if !status.IsSuccess() {
		return fmt.Errorf("binding failed: %s", status.Message())
	}

	return nil
}

func (sched *Scheduler) finishBinding(fwk framework.Framework, assumed *v1.Pod, targetNode string) {
	if finErr := sched.Cache.FinishBinding(assumed); finErr != nil {
		// logger.Error(finErr, "Scheduler cache FinishBinding failed")
	}
	// if !status.IsSuccess() {
	// 	// logger.V(1).Info("Failed to bind pod", "pod", klog.KObj(assumed))
	// 	return
	// }
}

// hasGPURequest checks if a pod requests GPU resources
func hasGPURequest(pod *v1.Pod) bool {
	for _, container := range pod.Spec.Containers {
		if _, ok := container.Resources.Requests["nvidia.com/gpu"]; ok {
			return true
		}
		if _, ok := container.Resources.Limits["nvidia.com/gpu"]; ok {
			return true
		}
	}
	return false
}
