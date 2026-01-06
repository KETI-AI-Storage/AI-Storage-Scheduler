package scheduler

import (
	"context"
	"fmt"
	"time"

	internalqueue "keti/ai-storage-scheduler/internal/backend/queue"
	logger "keti/ai-storage-scheduler/internal/backend/log"
	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

func (sched *Scheduler) ScheduleOne(ctx context.Context) {
	podInfo, err := sched.NextPod()

	if err != nil {
		return
	}

	if podInfo == nil || podInfo.Pod == nil {
		return
	}

	pod := podInfo.Pod

	fwk, err := sched.frameworkForPod(pod)
	if err != nil {
		sched.SchedulingQueue.Done(pod.UID)
		return
	}

	logger.Info("[scheduling] Starting scheduling cycle", 
		"namespace", pod.Namespace, "pod", pod.Name)

	start := time.Now()

	schedulingCycleCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	scheduleResult, assumedPodInfo, status := sched.schedulingCycle(schedulingCycleCtx, fwk, podInfo, start)
	if !status.IsSuccess() {
		logger.Warn("[scheduling] Scheduling cycle failed",
			"namespace", pod.Namespace, "pod", pod.Name, "reason", status.Message())
		return
	}

	logger.Info("[scheduling] Scheduling cycle succeeded",
		"namespace", pod.Namespace, "pod", pod.Name, 
		"node", scheduleResult.SuggestedHost,
		"duration_ms", time.Since(start).Milliseconds())

	// bind the pod to its host asynchronously
	go func() {
		bindingCycleCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		status := sched.bindingCycle(bindingCycleCtx, fwk, scheduleResult, assumedPodInfo, start)
		if !status.IsSuccess() {
			logger.Error("[scheduling] Binding cycle failed",
				fmt.Errorf("%s", status.Message()),
				"namespace", pod.Namespace, "pod", pod.Name)
			return
		}
		
		logger.Info("[scheduling] Pod successfully bound",
			"namespace", pod.Namespace, "pod", pod.Name, 
			"node", scheduleResult.SuggestedHost)
	}()
}

func (sched *Scheduler) frameworkForPod(pod *v1.Pod) (framework.Framework, error) {
	return sched.fwk, nil
}

var clearNominatedNode = &NominatingInfo{NominatingMode: ModeOverride, NominatedNodeName: ""}

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

		var nominatingInfo *NominatingInfo
		return ScheduleResult{nominatingInfo: nominatingInfo}, podInfo, utils.NewStatus(utils.Unschedulable).WithError(err)
	}

	assumedPodInfo := podInfo.DeepCopy()
	assumedPod := assumedPodInfo.Pod

	err = sched.assume(logger, assumedPod, scheduleResult.SuggestedHost)
	if err != nil {
		return ScheduleResult{nominatingInfo: clearNominatedNode}, assumedPodInfo, utils.AsStatus(err)
	}

	return scheduleResult, assumedPodInfo, nil
}

func (sched *Scheduler) assume(logger klog.Logger, assumed *v1.Pod, host string) error {
	assumed.Spec.NodeName = host

	if err := sched.Cache.AssumePod(logger, assumed); err != nil {
		logger.Error(err, "Scheduler cache AssumePod failed")
		return err
	}
	if sched.SchedulingQueue != nil {
		sched.SchedulingQueue.DeleteNominatedPodIfExists(assumed)
	}

	return nil
}

func (sched *Scheduler) bindingCycle(
	ctx context.Context,
	fwk framework.Framework,
	scheduleResult ScheduleResult,
	assumedPodInfo *internalqueue.QueuedPodInfo,
	start time.Time) *utils.Status {

	assumedPod := assumedPodInfo.Pod

	logger.Info("[binding] Starting binding cycle",
		"namespace", assumedPod.Namespace, "pod", assumedPod.Name,
		"node", scheduleResult.SuggestedHost)

	err := sched.runBindPlugin(ctx, fwk, assumedPod, &scheduleResult)
	if err != nil {
		logger.Error("[binding] Binding failed", err,
			"namespace", assumedPod.Namespace, "pod", assumedPod.Name)
		return utils.AsStatus(err)
	}

	sched.SchedulingQueue.Done(assumedPod.UID)

	return nil
}

func (sched *Scheduler) schedulePod(ctx context.Context, fwk framework.Framework, pod *v1.Pod) (result ScheduleResult, err error) {
	if sched.Cache.NodeCount() == 0 {
		return result, ErrNoNodesAvailable
	}

	// Check if pod requests GPU
	gpuRequest := int64(0)
	if hasGPURequest(pod) {
		for _, container := range pod.Spec.Containers {
			if gpu, ok := container.Resources.Requests["nvidia.com/gpu"]; ok {
				gpuRequest = gpu.Value()
			}
		}
		logger.Info("[scheduling] Pod requests GPU resources",
			"namespace", pod.Namespace, "pod", pod.Name, "gpu_count", gpuRequest)
		
		// Refresh metrics older than 60 seconds
		sched.refreshStaleGPUMetrics(ctx, 60000)
	}

	nodes := sched.Cache.Nodes()
	if nodes == nil {
		return result, fmt.Errorf("no nodes available")
	}
	
	logger.Info("[scheduling] Starting pod scheduling",
		"namespace", pod.Namespace, "pod", pod.Name,
		"total_nodes", len(nodes))
	
	scheduleResult := NewScheduleResult(nodes)

	// Filter phase
	logger.Info("[filter] Running filter plugins",
		"namespace", pod.Namespace, "pod", pod.Name)
	
	err = sched.runFilterPlugin(ctx, fwk, pod, &scheduleResult)
	if err != nil {
		return result, err
	}

	logger.Info("[filter] Filter phase completed",
		"namespace", pod.Namespace, "pod", pod.Name,
		"feasible_nodes", scheduleResult.FeasibleNodes,
		"total_nodes", len(nodes))

	if scheduleResult.FeasibleNodes == 0 {
		return result, fmt.Errorf("no nodes match pod requirements")
	} else if scheduleResult.FeasibleNodes == 1 {
		for name, pr := range scheduleResult.PluginResultMap {
			if !pr.IsFiltered {
				scheduleResult.SuggestedHost = name
				// Print ScoreMap even for single node
				singleNodeScoreMap := formatScoreMap(map[string]int{name: pr.TotalNodeScore})
				logger.Info("[ScoreMap] Node scores (single feasible node)",
					"namespace", pod.Namespace, "pod", pod.Name,
					"score_map", singleNodeScoreMap)
				logger.Info("[score] Only one feasible node, skipping score phase",
					"namespace", pod.Namespace, "pod", pod.Name,
					"selected_node", name, "score", pr.TotalNodeScore)
			}
		}
		return scheduleResult, nil
	}

	// Score phase
	logger.Info("[score] Running score plugins",
		"namespace", pod.Namespace, "pod", pod.Name,
		"feasible_nodes", scheduleResult.FeasibleNodes)
	
	err = sched.runScorePlugin(ctx, fwk, pod, &scheduleResult)
	if err != nil {
		return result, err
	}

	// Select best node
	err = sched.selectResource(pod, &scheduleResult)

	return scheduleResult, err
}

func (sched *Scheduler) runFilterPlugin(ctx context.Context, fwk framework.Framework, pod *v1.Pod, scheduleResult *ScheduleResult) error {
	nodes := sched.Cache.Nodes()
	feasibleNodes := 0
	filteredNodes := []string{}

	for nodeName, nodeInfo := range nodes {
		pluginResults := fwk.RunFilterPlugins(ctx, pod, nodeInfo)

		if pr, exists := pluginResults[nodeName]; exists {
			existingResult := scheduleResult.PluginResultMap[nodeName]
			existingResult.IsFiltered = pr.IsFiltered
			scheduleResult.PluginResultMap[nodeName] = existingResult

			if !pr.IsFiltered {
				feasibleNodes++
			} else {
				filteredNodes = append(filteredNodes, nodeName)
				// Log why node was filtered
				logger.Info("[filter] Node filtered out",
					"namespace", pod.Namespace, "pod", pod.Name,
					"node", nodeName, "reason", pr.FilterReason)
			}
		}
	}

	// Log filtered nodes summary
	if len(filteredNodes) > 0 {
		logger.Info("[filter] Nodes filtered out summary",
			"namespace", pod.Namespace, "pod", pod.Name,
			"filtered_count", len(filteredNodes),
			"filtered_nodes", filteredNodes)
	}

	scheduleResult.FeasibleNodes = feasibleNodes
	return nil
}

func (sched *Scheduler) runScorePlugin(ctx context.Context, fwk framework.Framework, pod *v1.Pod, scheduleResult *ScheduleResult) error {
	// Get list of feasible nodes
	feasibleNodes := []*v1.Node{}
	feasibleNodeNames := []string{}
	
	for nodeName, pr := range scheduleResult.PluginResultMap {
		if !pr.IsFiltered {
			if nodeInfo := sched.Cache.Nodes()[nodeName]; nodeInfo != nil && nodeInfo.Node() != nil {
				feasibleNodes = append(feasibleNodes, nodeInfo.Node())
				feasibleNodeNames = append(feasibleNodeNames, nodeName)
			}
		}
	}

	if len(feasibleNodes) == 0 {
		return fmt.Errorf("no feasible nodes")
	}

	logger.Info("[score] Scoring feasible nodes",
		"namespace", pod.Namespace, "pod", pod.Name,
		"nodes", feasibleNodeNames)

	// Run score plugins
	scores, status := fwk.RunScorePlugins(ctx, pod, feasibleNodes)
	if !status.IsSuccess() {
		return fmt.Errorf("scoring failed: %s", status.Message())
	}

	// Merge scores into scheduleResult and log scores
	for nodeName, score := range scores {
		if existing, ok := scheduleResult.PluginResultMap[nodeName]; ok {
			existing.Scores = score.Scores
			existing.TotalNodeScore = score.TotalNodeScore
			scheduleResult.PluginResultMap[nodeName] = existing
			
			// Log individual node scores
			logger.Info("[score] Node scored",
				"namespace", pod.Namespace, "pod", pod.Name,
				"node", nodeName, "total_score", score.TotalNodeScore)
		}
	}

	return nil
}

func (sched *Scheduler) selectResource(pod *v1.Pod, scheduleResult *ScheduleResult) error {
	// Select the node with the highest score
	var bestNode string
	var bestScore int = -1

	nodeScores := make(map[string]int)

	for nodeName, pr := range scheduleResult.PluginResultMap {
		if !pr.IsFiltered {
			nodeScores[nodeName] = pr.TotalNodeScore
			if pr.TotalNodeScore > bestScore {
				bestScore = pr.TotalNodeScore
				bestNode = nodeName
			}
		}
	}

	if bestNode == "" {
		return fmt.Errorf("no suitable node found")
	}

	// Print ScoreMap before final selection
	scoreMapStr := formatScoreMap(nodeScores)
	logger.Info("[ScoreMap] Node scores before selection",
		"namespace", pod.Namespace, "pod", pod.Name,
		"score_map", scoreMapStr)

	logger.Info("[score] Best node selected",
		"namespace", pod.Namespace, "pod", pod.Name,
		"selected_node", bestNode, "score", bestScore)

	scheduleResult.SuggestedHost = bestNode
	return nil
}

// formatScoreMap formats node scores as "[node1: score1, node2: score2, ...]"
func formatScoreMap(scores map[string]int) string {
	if len(scores) == 0 {
		return "[]"
	}

	result := "["
	first := true
	for nodeName, score := range scores {
		if !first {
			result += ", "
		}
		result += fmt.Sprintf("%s: %d", nodeName, score)
		first = false
	}
	result += "]"
	return result
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
		logger.Error("[binding] Cache FinishBinding failed", finErr,
			"namespace", assumed.Namespace, "pod", assumed.Name)
	}
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
