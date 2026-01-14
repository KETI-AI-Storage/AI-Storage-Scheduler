// ============================================
// KueueAware Plugin
// Kueue Workload CR 기반 Gang Scheduling 최적화
// ============================================
//
// 이 플러그인은 Kueue Workload CR을 직접 조회하여
// Gang Scheduling 및 Queue 기반 최적화를 제공합니다.
//
// 데이터 소스:
// - Kueue Workload CR (kueue.x-k8s.io/v1beta1)
//   - spec.podSets[].count: Gang 크기
//   - spec.queueName: LocalQueue 이름
//   - status.admission: 승인 정보 (flavor, 자원 할당)
//   - status.conditions: 상태 (Admitted, Finished 등)
//
// - Kueue LocalQueue CR
//   - spec.clusterQueue: ClusterQueue 참조
//
// - Kueue ClusterQueue CR
//   - status.pendingWorkloads: 대기 중인 워크로드 수
//   - status.admittedWorkloads: 승인된 워크로드 수
// ============================================

package plugin

import (
	"context"
	"strings"
	"sync"
	"time"

	v1api "keti/ai-storage-scheduler/api/v1"
	logger "keti/ai-storage-scheduler/internal/backend/log"
	"keti/ai-storage-scheduler/internal/configmanager"
	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const KueueAwareName = "KueueAware"

// Kueue GVRs
var (
	workloadGVR = schema.GroupVersionResource{
		Group:    "kueue.x-k8s.io",
		Version:  "v1beta1",
		Resource: "workloads",
	}
	localQueueGVR = schema.GroupVersionResource{
		Group:    "kueue.x-k8s.io",
		Version:  "v1beta1",
		Resource: "localqueues",
	}
	clusterQueueGVR = schema.GroupVersionResource{
		Group:    "kueue.x-k8s.io",
		Version:  "v1beta1",
		Resource: "clusterqueues",
	}
)

// KueueAware scores nodes based on Kueue workload characteristics
type KueueAware struct {
	cache         *utils.Cache
	kubeClient    kubernetes.Interface
	dynamicClient dynamic.Interface

	// Workload 캐시 (성능 최적화)
	workloadCache    map[string]*WorkloadInfo
	workloadCacheMu  sync.RWMutex
	workloadCacheTTL time.Duration
	lastCacheRefresh time.Time
}

var _ framework.ScorePlugin = &KueueAware{}

// WorkloadInfo contains parsed Kueue Workload information
type WorkloadInfo struct {
	Name         string
	Namespace    string
	QueueName    string
	ClusterQueue string

	// PodSets 정보
	PodSets       []PodSetInfo
	TotalPodCount int

	// Admission 정보
	IsAdmitted     bool
	AdmittedFlavor string

	// Status
	IsFinished bool

	// 조회 시간
	FetchedAt time.Time
}

type PodSetInfo struct {
	Name  string
	Count int
}

// NewKueueAware creates a new KueueAware plugin
func NewKueueAware(cache *utils.Cache, kubeClient kubernetes.Interface) *KueueAware {
	// Dynamic client 생성
	config, err := rest.InClusterConfig()
	var dynamicClient dynamic.Interface
	if err == nil {
		dynamicClient, _ = dynamic.NewForConfig(config)
	}

	return &KueueAware{
		cache:            cache,
		kubeClient:       kubeClient,
		dynamicClient:    dynamicClient,
		workloadCache:    make(map[string]*WorkloadInfo),
		workloadCacheTTL: 30 * time.Second,
	}
}

func (k *KueueAware) Name() string {
	return KueueAwareName
}

// Score scores nodes based on Kueue workload optimization
func (k *KueueAware) Score(ctx context.Context, pod *v1.Pod, nodeName string) (int64, *utils.Status) {
	nodeInfo := k.cache.Nodes()[nodeName]
	if nodeInfo == nil {
		return 0, utils.NewStatus(utils.Error, "node not found in cache")
	}

	config := configmanager.GetManager().GetKueueAwareConfig()
	if !config.Enabled {
		return 50, utils.NewStatus(utils.Success, "KueueAware disabled")
	}

	// Kueue Workload CR 조회
	workloadInfo := k.getWorkloadInfo(pod)
	if workloadInfo == nil {
		logger.Debug("[KueueAware] Pod is not Kueue-managed or Workload not found",
			"namespace", pod.Namespace, "pod", pod.Name)
		return 50, utils.NewStatus(utils.Success, "not Kueue-managed")
	}

	var totalScore int64 = 0

	// ═══════════════════════════════════════════════════════════════
	// 1. Gang Locality Score
	// 같은 Workload(Gang)의 다른 Pod가 있는 노드 선호
	// ═══════════════════════════════════════════════════════════════
	gangScore := k.calculateGangLocalityScore(pod, nodeInfo, workloadInfo, config.Scoring)
	totalScore += gangScore

	// ═══════════════════════════════════════════════════════════════
	// 2. Queue Priority Score
	// ClusterQueue의 상태 및 우선순위 기반 점수
	// ═══════════════════════════════════════════════════════════════
	queueScore := k.calculateQueuePriorityScore(ctx, workloadInfo, config.Scoring)
	totalScore += queueScore

	// ═══════════════════════════════════════════════════════════════
	// 3. Workload Size Score
	// Gang 크기(PodSets.Count)에 따른 노드 선호도
	// ═══════════════════════════════════════════════════════════════
	sizeScore := k.calculateWorkloadSizeScore(nodeInfo, workloadInfo, config.Scoring)
	totalScore += sizeScore

	// ═══════════════════════════════════════════════════════════════
	// 4. Flavor Affinity Score - 제거됨
	// Kueue가 nodeSelector를 자동 주입하므로 Filter 단계에서 처리됨
	// Score 단계에서 다시 계산할 필요 없음
	// ═══════════════════════════════════════════════════════════════

	logger.Info("[KueueAware] Scoring complete",
		"namespace", pod.Namespace,
		"pod", pod.Name,
		"node", nodeName,
		"workload", workloadInfo.Name,
		"queueName", workloadInfo.QueueName,
		"clusterQueue", workloadInfo.ClusterQueue,
		"gangSize", workloadInfo.TotalPodCount,
		"admitted", workloadInfo.IsAdmitted,
		"flavor", workloadInfo.AdmittedFlavor,
		"gangScore", gangScore,
		"queueScore", queueScore,
		"sizeScore", sizeScore,
		"totalScore", totalScore)

	return totalScore, utils.NewStatus(utils.Success, "")
}

func (k *KueueAware) ScoreExtensions() framework.ScoreExtensions {
	return k
}

func (k *KueueAware) NormalizeScore(ctx context.Context, pod *v1.Pod, scores utils.PluginResult) *utils.Status {
	return utils.NewStatus(utils.Success, "")
}

// ════════════════════════════════════════════════════════════════════════════
// Kueue Workload CR 조회
// ════════════════════════════════════════════════════════════════════════════

func (k *KueueAware) getWorkloadInfo(pod *v1.Pod) *WorkloadInfo {
	// Pod annotation에서 workload 이름 추출
	workloadName, ok := pod.Annotations["kueue.x-k8s.io/workload"]
	if !ok || workloadName == "" {
		return nil
	}

	cacheKey := pod.Namespace + "/" + workloadName

	// 캐시 확인
	k.workloadCacheMu.RLock()
	if cached, exists := k.workloadCache[cacheKey]; exists {
		if time.Since(cached.FetchedAt) < k.workloadCacheTTL {
			k.workloadCacheMu.RUnlock()
			return cached
		}
	}
	k.workloadCacheMu.RUnlock()

	// Workload CR 조회
	info := k.fetchWorkloadFromAPI(pod.Namespace, workloadName)
	if info != nil {
		k.workloadCacheMu.Lock()
		k.workloadCache[cacheKey] = info
		k.workloadCacheMu.Unlock()
	}

	return info
}

func (k *KueueAware) fetchWorkloadFromAPI(namespace, name string) *WorkloadInfo {
	if k.dynamicClient == nil {
		logger.Debug("[KueueAware] Dynamic client not available")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Workload CR 조회
	unstructuredWorkload, err := k.dynamicClient.Resource(workloadGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		logger.Debug("[KueueAware] Failed to get Workload CR", "error", err, "namespace", namespace, "name", name)
		return nil
	}

	info := &WorkloadInfo{
		Name:      name,
		Namespace: namespace,
		FetchedAt: time.Now(),
	}

	// spec.queueName
	if queueName, found, _ := unstructured.NestedString(unstructuredWorkload.Object, "spec", "queueName"); found {
		info.QueueName = queueName
	}

	// spec.podSets
	if podSets, found, _ := unstructured.NestedSlice(unstructuredWorkload.Object, "spec", "podSets"); found {
		for _, ps := range podSets {
			psMap, ok := ps.(map[string]interface{})
			if !ok {
				continue
			}
			podSetInfo := PodSetInfo{}
			if psName, ok := psMap["name"].(string); ok {
				podSetInfo.Name = psName
			}
			if count, ok := psMap["count"].(int64); ok {
				podSetInfo.Count = int(count)
			} else if countFloat, ok := psMap["count"].(float64); ok {
				podSetInfo.Count = int(countFloat)
			}
			info.PodSets = append(info.PodSets, podSetInfo)
			info.TotalPodCount += podSetInfo.Count
		}
	}

	// status.admission
	if admission, found, _ := unstructured.NestedMap(unstructuredWorkload.Object, "status", "admission"); found {
		info.IsAdmitted = true

		// admission.clusterQueue
		if cq, ok := admission["clusterQueue"].(string); ok {
			info.ClusterQueue = cq
		}

		// admission.podSetAssignments[0].flavors
		if podSetAssignments, ok := admission["podSetAssignments"].([]interface{}); ok && len(podSetAssignments) > 0 {
			if firstAssignment, ok := podSetAssignments[0].(map[string]interface{}); ok {
				if flavors, ok := firstAssignment["flavors"].(map[string]interface{}); ok {
					// 첫 번째 flavor 이름 추출
					for _, flavorName := range flavors {
						if fn, ok := flavorName.(string); ok {
							info.AdmittedFlavor = fn
							break
						}
					}
				}
			}
		}
	}

	// status.conditions - Finished 확인
	if conditions, found, _ := unstructured.NestedSlice(unstructuredWorkload.Object, "status", "conditions"); found {
		for _, cond := range conditions {
			condMap, ok := cond.(map[string]interface{})
			if !ok {
				continue
			}
			if condType, ok := condMap["type"].(string); ok && condType == "Finished" {
				if status, ok := condMap["status"].(string); ok && status == "True" {
					info.IsFinished = true
				}
			}
		}
	}

	// LocalQueue에서 ClusterQueue 이름 조회 (아직 없으면)
	if info.ClusterQueue == "" && info.QueueName != "" {
		info.ClusterQueue = k.getClusterQueueFromLocalQueue(ctx, namespace, info.QueueName)
	}

	logger.Debug("[KueueAware] Fetched Workload CR",
		"namespace", namespace,
		"name", name,
		"queueName", info.QueueName,
		"clusterQueue", info.ClusterQueue,
		"totalPodCount", info.TotalPodCount,
		"isAdmitted", info.IsAdmitted,
		"admittedFlavor", info.AdmittedFlavor)

	return info
}

func (k *KueueAware) getClusterQueueFromLocalQueue(ctx context.Context, namespace, queueName string) string {
	if k.dynamicClient == nil {
		return ""
	}

	localQueue, err := k.dynamicClient.Resource(localQueueGVR).Namespace(namespace).Get(ctx, queueName, metav1.GetOptions{})
	if err != nil {
		return ""
	}

	if cq, found, _ := unstructured.NestedString(localQueue.Object, "spec", "clusterQueue"); found {
		return cq
	}

	return ""
}

// ════════════════════════════════════════════════════════════════════════════
// Gang Locality Score 계산
// 워크로드 크기와 GPU 요청에 따라 배치 전략이 달라짐:
// - 작은 Gang (1-4 pods): 같은 노드 선호 (네트워크 효율)
// - 큰 Gang (5+ pods): 분산 배치 선호 (리소스 최대화)
// - GPU 워크로드: 분산 배치 강하게 선호 (GPU 자원 최대 활용)
// ════════════════════════════════════════════════════════════════════════════

func (k *KueueAware) calculateGangLocalityScore(pod *v1.Pod, nodeInfo *utils.NodeInfo, workloadInfo *WorkloadInfo, scoring v1api.KueueAwareScoringConfig) int64 {
	maxScore := int64(scoring.GangLocalityScoreMax)
	if maxScore == 0 {
		maxScore = 30
	}

	// Gang이 1개면 locality 의미 없음
	if workloadInfo.TotalPodCount <= 1 {
		return maxScore / 2
	}

	gangMembersOnNode := 0

	// 현재 노드에 있는 같은 Workload의 Pod 수 계산
	for _, podInfo := range nodeInfo.Pods {
		if podInfo.Pod == nil {
			continue
		}
		// 같은 workload인지 확인
		if wl, ok := podInfo.Pod.Annotations["kueue.x-k8s.io/workload"]; ok {
			if wl == workloadInfo.Name && podInfo.Pod.Namespace == workloadInfo.Namespace {
				gangMembersOnNode++
			}
		}
	}

	// GPU 요청 확인
	requiresGPU := k.podRequiresGPU(pod)

	// 배치 전략 결정
	// 큰 Gang (5+) 또는 GPU 워크로드: 분산 배치 (Spread)
	// 작은 Gang (1-4): 같은 노드 선호 (Co-location)
	useSpreadStrategy := workloadInfo.TotalPodCount >= 5 || requiresGPU

	if useSpreadStrategy {
		// ═══════════════════════════════════════════════════════════════
		// Spread 전략: 노드당 Pod 수가 적을수록 높은 점수
		// 분산 학습에서 GPU/리소스를 최대한 활용하기 위함
		// ═══════════════════════════════════════════════════════════════

		// 이상적인 노드당 Pod 수 계산 (균등 분배 기준)
		// 예: 8 pods, 4 nodes → idealPerNode = 2
		totalNodes := len(k.cache.Nodes())
		if totalNodes == 0 {
			totalNodes = 1
		}
		idealPerNode := (workloadInfo.TotalPodCount + totalNodes - 1) / totalNodes
		if idealPerNode < 1 {
			idealPerNode = 1
		}

		// 현재 노드의 Gang 멤버 수가 적을수록 높은 점수
		if gangMembersOnNode == 0 {
			// 아직 Gang 멤버가 없는 노드 → 최고 점수
			return maxScore
		} else if gangMembersOnNode < idealPerNode {
			// 이상적인 수보다 적음 → 높은 점수
			ratio := 1.0 - (float64(gangMembersOnNode) / float64(idealPerNode))
			return int64(float64(maxScore) * ratio * 0.8)
		} else {
			// 이미 충분히 있음 → 낮은 점수 (다른 노드로 분산 유도)
			return maxScore / 5
		}
	} else {
		// ═══════════════════════════════════════════════════════════════
		// Co-location 전략: 같은 노드에 모을수록 높은 점수
		// 작은 Gang의 네트워크 효율을 위함
		// ═══════════════════════════════════════════════════════════════

		if workloadInfo.TotalPodCount > 0 {
			ratio := float64(gangMembersOnNode) / float64(workloadInfo.TotalPodCount)
			score := int64(float64(maxScore) * ratio)

			// 최소 1명이라도 있으면 보너스
			if gangMembersOnNode > 0 {
				score += maxScore / 5
			}

			return minInt64(score, maxScore)
		}
	}

	return 0
}

// podRequiresGPU checks if the pod requests GPU resources
func (k *KueueAware) podRequiresGPU(pod *v1.Pod) bool {
	for _, container := range pod.Spec.Containers {
		if container.Resources.Requests != nil {
			for resourceName := range container.Resources.Requests {
				// nvidia.com/gpu, amd.com/gpu, etc.
				if strings.Contains(string(resourceName), "gpu") {
					return true
				}
			}
		}
		if container.Resources.Limits != nil {
			for resourceName := range container.Resources.Limits {
				if strings.Contains(string(resourceName), "gpu") {
					return true
				}
			}
		}
	}
	return false
}

// ════════════════════════════════════════════════════════════════════════════
// Queue Priority Score 계산
// ════════════════════════════════════════════════════════════════════════════

func (k *KueueAware) calculateQueuePriorityScore(ctx context.Context, workloadInfo *WorkloadInfo, scoring v1api.KueueAwareScoringConfig) int64 {
	maxScore := int64(scoring.QueuePriorityScoreMax)
	if maxScore == 0 {
		maxScore = 20
	}

	if workloadInfo.ClusterQueue == "" {
		return maxScore / 2
	}

	// ClusterQueue 상태 조회
	cqStatus := k.getClusterQueueStatus(ctx, workloadInfo.ClusterQueue)
	if cqStatus == nil {
		return maxScore / 2
	}

	// 대기 워크로드가 많으면 점수 낮춤 (혼잡한 queue)
	// 대기가 적으면 점수 높임 (여유있는 queue)
	pendingRatio := float64(cqStatus.PendingWorkloads) / float64(maxInt(cqStatus.PendingWorkloads+cqStatus.AdmittedWorkloads, 1))

	// 대기 비율이 낮을수록 높은 점수
	score := int64(float64(maxScore) * (1.0 - pendingRatio))

	logger.Debug("[KueueAware] Queue priority score",
		"clusterQueue", workloadInfo.ClusterQueue,
		"pending", cqStatus.PendingWorkloads,
		"admitted", cqStatus.AdmittedWorkloads,
		"score", score)

	return score
}

type ClusterQueueStatus struct {
	PendingWorkloads  int
	AdmittedWorkloads int
}

func (k *KueueAware) getClusterQueueStatus(ctx context.Context, cqName string) *ClusterQueueStatus {
	if k.dynamicClient == nil {
		return nil
	}

	cq, err := k.dynamicClient.Resource(clusterQueueGVR).Get(ctx, cqName, metav1.GetOptions{})
	if err != nil {
		return nil
	}

	status := &ClusterQueueStatus{}

	if pending, found, _ := unstructured.NestedInt64(cq.Object, "status", "pendingWorkloads"); found {
		status.PendingWorkloads = int(pending)
	}
	if admitted, found, _ := unstructured.NestedInt64(cq.Object, "status", "admittedWorkloads"); found {
		status.AdmittedWorkloads = int(admitted)
	}

	return status
}

// ════════════════════════════════════════════════════════════════════════════
// Workload Size Score 계산
// ════════════════════════════════════════════════════════════════════════════

func (k *KueueAware) calculateWorkloadSizeScore(nodeInfo *utils.NodeInfo, workloadInfo *WorkloadInfo, scoring v1api.KueueAwareScoringConfig) int64 {
	maxScore := int64(scoring.WorkloadSizeScoreMax)
	if maxScore == 0 {
		maxScore = 25
	}

	node := nodeInfo.Node()
	if node == nil {
		return 0
	}

	// 대규모 Gang (3개 이상)은 자원이 여유있는 노드 선호
	isLargeGang := workloadInfo.TotalPodCount >= 3

	if isLargeGang {
		allocatable := node.Status.Allocatable
		cpuAllocatable := allocatable.Cpu().MilliValue()
		memAllocatable := allocatable.Memory().Value()

		cpuUsed := nodeInfo.Requested.MilliCPU
		memUsed := nodeInfo.Requested.Memory

		cpuAvailable := cpuAllocatable - cpuUsed
		memAvailable := memAllocatable - memUsed

		// 여유 자원 비율
		cpuRatio := float64(cpuAvailable) / float64(maxInt64(cpuAllocatable, 1))
		memRatio := float64(memAvailable) / float64(maxInt64(memAllocatable, 1))
		avgRatio := (cpuRatio + memRatio) / 2

		return int64(float64(maxScore) * avgRatio)
	}

	// 소규모 Gang은 중간 점수
	return maxScore / 2
}

// ════════════════════════════════════════════════════════════════════════════
// Helper functions
// ════════════════════════════════════════════════════════════════════════════

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
