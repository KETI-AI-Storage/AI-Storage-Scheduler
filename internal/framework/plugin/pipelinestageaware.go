// ============================================
// PipelineStageAware Plugin
// Kubeflow/Argo Pipeline 스테이지 간 데이터 지역성 최적화
// ============================================
//
// 이 플러그인은 파이프라인의 연속된 스테이지를 같은 노드에 배치하여
// 데이터 전송을 최소화합니다.
//
// 주요 기능:
// 1. Colocation: 이전 스테이지와 같은 노드 배치 시도
// 2. Rack Fallback: 같은 노드 불가 시 같은 랙 노드 선호
// 3. Soft Reserve: 다음 스테이지를 위한 리소스 사전 예약 (annotation)
//
// 데이터 소스:
// - Pod Labels:
//   - workflows.argoproj.io/workflow: 워크플로우 이름
// - Pod Annotations:
//   - workflows.argoproj.io/node-name: 현재 스텝 이름
//   - workflows.argoproj.io/node-id: 스텝 고유 ID
// - Node Labels:
//   - topology.kubernetes.io/rack: 랙 정보
//   - topology.kubernetes.io/zone: 존 정보
// - Argo Workflow CR:
//   - spec.templates[].dag.tasks[].dependencies: DAG 의존성
//   - status.nodes[].hostNodeName: 각 스텝이 실행된 노드
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

const PipelineStageAwareName = "PipelineStageAware"

// Argo Workflow GVR
var workflowGVR = schema.GroupVersionResource{
	Group:    "argoproj.io",
	Version:  "v1alpha1",
	Resource: "workflows",
}

// PipelineStageAware scores nodes based on pipeline stage data locality
type PipelineStageAware struct {
	cache         *utils.Cache
	kubeClient    kubernetes.Interface
	dynamicClient dynamic.Interface

	// Workflow 캐시 (성능 최적화)
	workflowCache    map[string]*WorkflowInfo
	workflowCacheMu  sync.RWMutex
	workflowCacheTTL time.Duration
}

var _ framework.ScorePlugin = &PipelineStageAware{}

// WorkflowInfo contains parsed Argo Workflow information
type WorkflowInfo struct {
	Name      string
	Namespace string

	// DAG 구조: task name -> dependencies
	DAGDependencies map[string][]string

	// 각 스텝의 실행 노드: node-name -> hostNodeName
	StepNodeMap map[string]string

	// 조회 시간
	FetchedAt time.Time
}

// NewPipelineStageAware creates a new PipelineStageAware plugin
func NewPipelineStageAware(cache *utils.Cache, kubeClient kubernetes.Interface) *PipelineStageAware {
	// Dynamic client 생성
	config, err := rest.InClusterConfig()
	var dynamicClient dynamic.Interface
	if err == nil {
		dynamicClient, _ = dynamic.NewForConfig(config)
	}

	return &PipelineStageAware{
		cache:            cache,
		kubeClient:       kubeClient,
		dynamicClient:    dynamicClient,
		workflowCache:    make(map[string]*WorkflowInfo),
		workflowCacheTTL: 30 * time.Second,
	}
}

func (p *PipelineStageAware) Name() string {
	return PipelineStageAwareName
}

// Score scores nodes based on pipeline stage data locality
func (p *PipelineStageAware) Score(ctx context.Context, pod *v1.Pod, nodeName string) (int64, *utils.Status) {
	nodeInfo := p.cache.Nodes()[nodeName]
	if nodeInfo == nil {
		return 0, utils.NewStatus(utils.Error, "node not found in cache")
	}

	config := configmanager.GetManager().GetPipelineStageAwareConfig()
	if !config.Enabled {
		return 50, utils.NewStatus(utils.Success, "PipelineStageAware disabled")
	}

	// Argo Workflow 정보 확인
	workflowName := p.getWorkflowName(pod)
	if workflowName == "" {
		logger.Debug("[PipelineStageAware] Pod is not part of Argo Workflow",
			"namespace", pod.Namespace, "pod", pod.Name)
		return 50, utils.NewStatus(utils.Success, "not Argo workflow pod")
	}

	// 현재 스텝 이름 확인
	currentStepName := p.getCurrentStepName(pod)
	if currentStepName == "" {
		return 50, utils.NewStatus(utils.Success, "step name not found")
	}

	// Workflow CR 조회
	workflowInfo := p.getWorkflowInfo(pod.Namespace, workflowName)
	if workflowInfo == nil {
		logger.Debug("[PipelineStageAware] Workflow not found",
			"namespace", pod.Namespace, "workflow", workflowName)
		return 50, utils.NewStatus(utils.Success, "workflow not found")
	}

	// ═══════════════════════════════════════════════════════════════
	// Phase 1: 이전 스테이지 컨텍스트 확인
	// ═══════════════════════════════════════════════════════════════
	dependencies := workflowInfo.DAGDependencies[currentStepName]
	previousStageNode := ""
	previousStageName := ""

	if len(dependencies) > 0 {
		previousStageName = dependencies[0] // 첫 번째 dependency
		if node, exists := workflowInfo.StepNodeMap[previousStageName]; exists {
			previousStageNode = node
		}
	}

	logger.Info("[PipelineStageAware] ========== PIPELINE PLACEMENT ATTEMPT ==========")
	logger.Info("[PipelineStageAware] Pipeline Flow:",
		"workflow", workflowName,
		"currentStep", currentStepName,
		"previousStep", previousStageName,
		"previousNode", previousStageNode)

	var totalScore int64 = 0

	// ═══════════════════════════════════════════════════════════════
	// Phase 2: Colocation 시도 - 이전 스테이지와 같은 노드
	// ═══════════════════════════════════════════════════════════════
	colocationScore, isExactMatch := p.calculateColocationScore(nodeName, previousStageNode, nodeInfo, config.Scoring)
	totalScore += colocationScore

	if isExactMatch {
		logger.Info("[PipelineStageAware] [Attempt 1] Colocation SUCCESS",
			"targetNode", nodeName,
			"previousStageNode", previousStageNode,
			"score", colocationScore)
	} else if previousStageNode != "" {
		// ═══════════════════════════════════════════════════════════════
		// Phase 3: Rack Fallback - 같은 랙 노드 시도
		// ═══════════════════════════════════════════════════════════════
		rackScore := p.calculateRackLocalityScore(nodeName, previousStageNode, nodeInfo, config.Scoring)
		totalScore += rackScore

		if rackScore > 0 {
			logger.Info("[PipelineStageAware] [Attempt 2] Rack Fallback",
				"targetNode", nodeName,
				"previousStageNode", previousStageNode,
				"sameRack", rackScore > 0,
				"rackScore", rackScore)
		}
	}

	// ═══════════════════════════════════════════════════════════════
	// Phase 4: Pipeline Cohesion Score
	// ═══════════════════════════════════════════════════════════════
	cohesionScore := p.calculatePipelineCohesionScore(nodeName, workflowInfo, config.Scoring)
	totalScore += cohesionScore

	// ═══════════════════════════════════════════════════════════════
	// Phase 5: I/O Pattern Score
	// ═══════════════════════════════════════════════════════════════
	ioScore := p.calculateIOPatternScore(pod, nodeInfo, config.Scoring)
	totalScore += ioScore

	// ═══════════════════════════════════════════════════════════════
	// Phase 6: 다음 스테이지 Soft Reserve 정보 (로그용)
	// ═══════════════════════════════════════════════════════════════
	nextStages := p.getNextStages(currentStepName, workflowInfo)
	if len(nextStages) > 0 && totalScore > 50 {
		// 높은 점수를 받은 노드에 대해 다음 스테이지 예약 정보 출력
		logger.Info("[PipelineStageAware] Soft-Reserve candidate for next stages",
			"node", nodeName,
			"currentStep", currentStepName,
			"nextStages", nextStages,
			"reservePriority", "medium")
	}

	// ═══════════════════════════════════════════════════════════════
	// 결과 Summary 로그
	// ═══════════════════════════════════════════════════════════════
	logger.Info("[PipelineStageAware] ========== PIPELINE PLACEMENT SUMMARY ==========")
	logger.Info("[PipelineStageAware] Pipeline Flow:",
		"previousStep", previousStageName,
		"previousNode", previousStageNode,
		"currentStep", currentStepName,
		"targetNode", nodeName)

	if len(nextStages) > 0 {
		logger.Info("[PipelineStageAware] Next stages to schedule:", "stages", nextStages)
	}

	logger.Info("[PipelineStageAware] Scoring breakdown",
		"colocationScore", colocationScore,
		"isExactMatch", isExactMatch,
		"cohesionScore", cohesionScore,
		"ioScore", ioScore,
		"totalScore", totalScore)

	return totalScore, utils.NewStatus(utils.Success, "")
}

func (p *PipelineStageAware) ScoreExtensions() framework.ScoreExtensions {
	return p
}

func (p *PipelineStageAware) NormalizeScore(ctx context.Context, pod *v1.Pod, scores utils.PluginResult) *utils.Status {
	return utils.NewStatus(utils.Success, "")
}

// ════════════════════════════════════════════════════════════════════════════
// Workflow 정보 조회
// ════════════════════════════════════════════════════════════════════════════

func (p *PipelineStageAware) getWorkflowName(pod *v1.Pod) string {
	// Label에서 workflow 이름 추출
	if wfName, ok := pod.Labels["workflows.argoproj.io/workflow"]; ok {
		return wfName
	}
	return ""
}

func (p *PipelineStageAware) getCurrentStepName(pod *v1.Pod) string {
	// Annotation에서 현재 스텝 이름 추출
	// 형식: "workflow-name.step-name"
	if nodeName, ok := pod.Annotations["workflows.argoproj.io/node-name"]; ok {
		// workflow-name.step-name에서 step-name만 추출
		parts := strings.Split(nodeName, ".")
		if len(parts) >= 2 {
			return parts[len(parts)-1]
		}
		return nodeName
	}
	return ""
}

func (p *PipelineStageAware) getWorkflowInfo(namespace, name string) *WorkflowInfo {
	cacheKey := namespace + "/" + name

	// 캐시 확인
	p.workflowCacheMu.RLock()
	if cached, exists := p.workflowCache[cacheKey]; exists {
		if time.Since(cached.FetchedAt) < p.workflowCacheTTL {
			p.workflowCacheMu.RUnlock()
			return cached
		}
	}
	p.workflowCacheMu.RUnlock()

	// Workflow CR 조회
	info := p.fetchWorkflowFromAPI(namespace, name)
	if info != nil {
		p.workflowCacheMu.Lock()
		p.workflowCache[cacheKey] = info
		p.workflowCacheMu.Unlock()
	}

	return info
}

func (p *PipelineStageAware) fetchWorkflowFromAPI(namespace, name string) *WorkflowInfo {
	if p.dynamicClient == nil {
		logger.Debug("[PipelineStageAware] Dynamic client not available")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Workflow CR 조회
	unstructuredWf, err := p.dynamicClient.Resource(workflowGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		logger.Debug("[PipelineStageAware] Failed to get Workflow CR",
			"error", err, "namespace", namespace, "name", name)
		return nil
	}

	info := &WorkflowInfo{
		Name:            name,
		Namespace:       namespace,
		DAGDependencies: make(map[string][]string),
		StepNodeMap:     make(map[string]string),
		FetchedAt:       time.Now(),
	}

	// spec.templates[].dag.tasks 파싱
	templates, found, _ := unstructured.NestedSlice(unstructuredWf.Object, "spec", "templates")
	if found {
		for _, tmpl := range templates {
			tmplMap, ok := tmpl.(map[string]interface{})
			if !ok {
				continue
			}

			// DAG 템플릿 찾기
			dag, found, _ := unstructured.NestedMap(tmplMap, "dag")
			if !found {
				continue
			}

			tasks, found, _ := unstructured.NestedSlice(dag, "tasks")
			if !found {
				continue
			}

			for _, task := range tasks {
				taskMap, ok := task.(map[string]interface{})
				if !ok {
					continue
				}

				taskName, _ := taskMap["name"].(string)
				if taskName == "" {
					continue
				}

				// dependencies 파싱
				deps, found, _ := unstructured.NestedStringSlice(taskMap, "dependencies")
				if found {
					info.DAGDependencies[taskName] = deps
				} else {
					info.DAGDependencies[taskName] = []string{}
				}
			}
		}
	}

	// status.nodes 파싱 (각 스텝의 실행 노드)
	nodes, found, _ := unstructured.NestedMap(unstructuredWf.Object, "status", "nodes")
	if found {
		for _, nodeData := range nodes {
			nodeMap, ok := nodeData.(map[string]interface{})
			if !ok {
				continue
			}

			// Pod 타입만 처리
			nodeType, _ := nodeMap["type"].(string)
			if nodeType != "Pod" {
				continue
			}

			displayName, _ := nodeMap["displayName"].(string)
			hostNodeName, _ := nodeMap["hostNodeName"].(string)

			if displayName != "" && hostNodeName != "" {
				info.StepNodeMap[displayName] = hostNodeName
			}
		}
	}

	logger.Debug("[PipelineStageAware] Fetched Workflow CR",
		"namespace", namespace,
		"name", name,
		"dagTasks", len(info.DAGDependencies),
		"executedSteps", len(info.StepNodeMap))

	return info
}

// ════════════════════════════════════════════════════════════════════════════
// Colocation Score 계산
// 이전 스테이지와 정확히 같은 노드에 배치 시 최고 점수
// ════════════════════════════════════════════════════════════════════════════

func (p *PipelineStageAware) calculateColocationScore(nodeName, previousStageNode string, nodeInfo *utils.NodeInfo, scoring v1api.PipelineStageAwareScoringConfig) (int64, bool) {
	maxScore := int64(scoring.DependencyLocalityScoreMax)
	if maxScore == 0 {
		maxScore = 40
	}

	// 첫 번째 스텝 (이전 스테이지 없음)
	if previousStageNode == "" {
		return maxScore / 2, false
	}

	// 정확히 같은 노드인지 확인
	if nodeName == previousStageNode {
		logger.Debug("[PipelineStageAware] Exact colocation match",
			"node", nodeName,
			"previousStageNode", previousStageNode,
			"score", maxScore)
		return maxScore, true
	}

	return 0, false
}

// ════════════════════════════════════════════════════════════════════════════
// Rack Locality Score 계산
// 같은 랙에 있는 노드에 가산점 (Colocation 실패 시 Fallback)
// ════════════════════════════════════════════════════════════════════════════

func (p *PipelineStageAware) calculateRackLocalityScore(nodeName, previousStageNode string, nodeInfo *utils.NodeInfo, scoring v1api.PipelineStageAwareScoringConfig) int64 {
	// Rack fallback은 DependencyLocalityScoreMax의 70% 정도
	maxScore := int64(scoring.DependencyLocalityScoreMax)
	if maxScore == 0 {
		maxScore = 40
	}
	rackMaxScore := maxScore * 7 / 10

	if previousStageNode == "" {
		return 0
	}

	// 현재 노드의 랙 정보
	currentNode := nodeInfo.Node()
	if currentNode == nil {
		return 0
	}

	currentRack := p.getNodeRack(currentNode)
	if currentRack == "" {
		return 0
	}

	// 이전 스테이지 노드의 랙 정보
	previousNodeInfo := p.cache.Nodes()[previousStageNode]
	if previousNodeInfo == nil {
		return 0
	}

	previousNode := previousNodeInfo.Node()
	if previousNode == nil {
		return 0
	}

	previousRack := p.getNodeRack(previousNode)
	if previousRack == "" {
		return 0
	}

	// 같은 랙인지 확인
	if currentRack == previousRack {
		logger.Debug("[PipelineStageAware] Same rack fallback",
			"node", nodeName,
			"previousNode", previousStageNode,
			"rack", currentRack,
			"score", rackMaxScore)
		return rackMaxScore
	}

	// 다른 랙이지만 같은 존(Zone)인지 확인
	currentZone := p.getNodeZone(currentNode)
	previousZone := p.getNodeZone(previousNode)

	if currentZone != "" && currentZone == previousZone {
		zoneScore := rackMaxScore / 2
		logger.Debug("[PipelineStageAware] Same zone fallback",
			"node", nodeName,
			"previousNode", previousStageNode,
			"zone", currentZone,
			"score", zoneScore)
		return zoneScore
	}

	return 0
}

// getNodeRack returns the rack label of a node
func (p *PipelineStageAware) getNodeRack(node *v1.Node) string {
	if node == nil {
		return ""
	}

	// 표준 topology 라벨
	if rack, ok := node.Labels["topology.kubernetes.io/rack"]; ok {
		return rack
	}
	// 커스텀 라벨
	if rack, ok := node.Labels["rack"]; ok {
		return rack
	}
	if rack, ok := node.Labels["failure-domain.beta.kubernetes.io/rack"]; ok {
		return rack
	}

	return ""
}

// getNodeZone returns the zone label of a node
func (p *PipelineStageAware) getNodeZone(node *v1.Node) string {
	if node == nil {
		return ""
	}

	if zone, ok := node.Labels["topology.kubernetes.io/zone"]; ok {
		return zone
	}
	if zone, ok := node.Labels["failure-domain.beta.kubernetes.io/zone"]; ok {
		return zone
	}

	return ""
}

// getNextStages returns the stages that depend on the current stage
func (p *PipelineStageAware) getNextStages(currentStep string, workflowInfo *WorkflowInfo) []string {
	if workflowInfo == nil {
		return nil
	}

	var nextStages []string
	for stageName, deps := range workflowInfo.DAGDependencies {
		for _, dep := range deps {
			if dep == currentStep {
				nextStages = append(nextStages, stageName)
				break
			}
		}
	}

	return nextStages
}

// ════════════════════════════════════════════════════════════════════════════
// Pipeline Cohesion Score 계산
// 같은 파이프라인의 스텝들이 많이 실행된 노드 선호
// ════════════════════════════════════════════════════════════════════════════

func (p *PipelineStageAware) calculatePipelineCohesionScore(nodeName string, workflowInfo *WorkflowInfo, scoring v1api.PipelineStageAwareScoringConfig) int64 {
	maxScore := int64(scoring.PipelineCohesionScoreMax)
	if maxScore == 0 {
		maxScore = 30
	}

	if len(workflowInfo.StepNodeMap) == 0 {
		return maxScore / 2
	}

	// 해당 노드에서 실행된 스텝 수 계산
	stepsOnNode := 0
	for _, hostNode := range workflowInfo.StepNodeMap {
		if hostNode == nodeName {
			stepsOnNode++
		}
	}

	if stepsOnNode == 0 {
		// 이 노드에서 실행된 스텝이 없음
		return 0
	}

	// 전체 대비 비율로 점수 계산
	ratio := float64(stepsOnNode) / float64(len(workflowInfo.StepNodeMap))
	score := int64(float64(maxScore) * ratio)

	// 최소 1개라도 있으면 보너스
	if stepsOnNode > 0 {
		score += maxScore / 5
	}

	if score > maxScore {
		score = maxScore
	}

	return score
}

// ════════════════════════════════════════════════════════════════════════════
// I/O Pattern Score 계산
// 워크로드 타입에 따른 최적 노드 선택
// ════════════════════════════════════════════════════════════════════════════

func (p *PipelineStageAware) calculateIOPatternScore(pod *v1.Pod, nodeInfo *utils.NodeInfo, scoring v1api.PipelineStageAwareScoringConfig) int64 {
	maxScore := int64(scoring.IOPatternScoreMax)
	if maxScore == 0 {
		maxScore = 30
	}

	// Pod annotation에서 워크로드 타입 확인
	workloadType := ""
	if wt, ok := pod.Annotations["insight-trace/workload-type"]; ok {
		workloadType = wt
	}

	ioPattern := ""
	if io, ok := pod.Annotations["ai-storage.keti/io-pattern"]; ok {
		ioPattern = io
	}

	node := nodeInfo.Node()
	if node == nil {
		return maxScore / 2
	}

	// 노드의 스토리지 티어 확인
	nodeTier := ""
	if tier, ok := node.Labels["storage.tier"]; ok {
		nodeTier = tier
	} else if tier, ok := node.Labels["storage.exastor.io/tier"]; ok {
		nodeTier = tier
	}

	score := maxScore / 2 // 기본 점수

	// 워크로드 타입과 노드 티어 매칭
	switch workloadType {
	case "preprocessing", "training":
		// 전처리/학습은 Hot 티어 선호
		if nodeTier == "hot" || nodeTier == "nvme" {
			score = maxScore
		} else if nodeTier == "warm" || nodeTier == "ssd" {
			score = maxScore * 7 / 10
		}
	case "inference":
		// 추론은 Warm 티어도 OK
		if nodeTier == "warm" || nodeTier == "ssd" || nodeTier == "hot" || nodeTier == "nvme" {
			score = maxScore
		}
	case "archiving":
		// 아카이빙은 Cold 티어 선호
		if nodeTier == "cold" || nodeTier == "hdd" {
			score = maxScore
		}
	}

	// I/O 패턴 추가 고려
	switch ioPattern {
	case "write-heavy":
		if nodeTier == "hot" || nodeTier == "nvme" {
			score += maxScore / 5
		}
	case "read-heavy":
		// 읽기 집약은 캐시가 있는 노드 선호 (추후 확장)
	}

	if score > maxScore {
		score = maxScore
	}

	return score
}
