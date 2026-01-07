// ============================================
// StorageTierAware Plugin
// I/O 패턴 고려, NVMe/SSD/HDD 티어 선택
// ============================================
//
// 이 플러그인은 APOLLO로부터 워크로드의 I/O 패턴 분석 결과를 받아
// 적합한 스토리지 티어를 가진 노드에 높은 점수를 부여합니다.
//
// APOLLO 연동:
// - insight-trace가 분석한 I/O 패턴 (순차/랜덤/버스티)
// - 권장 스토리지 클래스 (NVMe/SSD/HDD/CSD)
// - 최소 IOPS/처리량 요구사항
//
// 스토리지 티어 우선순위:
// 1. NVMe - 고성능 랜덤 I/O, 대용량 순차 읽기/쓰기
// 2. SSD  - 일반적인 랜덤 I/O, 중간 규모 작업
// 3. HDD  - 순차 I/O 중심, 대용량 저장
// 4. CSD  - 컴퓨팅 스토리지, 데이터 처리 오프로드
// ============================================

package plugin

import (
	"context"
	"strconv"
	"strings"

	"keti/ai-storage-scheduler/internal/apollo"
	logger "keti/ai-storage-scheduler/internal/backend/log"
	"keti/ai-storage-scheduler/internal/configmanager"
	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

const StorageTierAwareName = "StorageTierAware"

// StorageTier represents storage device tiers
type StorageTier int

const (
	StorageTierUnknown StorageTier = iota
	StorageTierHDD                 // Spinning disk
	StorageTierSSD                 // SATA/SAS SSD
	StorageTierNVMe                // NVMe SSD
	StorageTierCSD                 // Computational Storage Device
	StorageTierMemory              // Memory-based storage (tmpfs, RAM disk)
)

func (t StorageTier) String() string {
	switch t {
	case StorageTierHDD:
		return "HDD"
	case StorageTierSSD:
		return "SSD"
	case StorageTierNVMe:
		return "NVMe"
	case StorageTierCSD:
		return "CSD"
	case StorageTierMemory:
		return "Memory"
	default:
		return "Unknown"
	}
}

// StorageTierAware scores nodes based on storage tier matching for workload I/O patterns
type StorageTierAware struct {
	cache        *utils.Cache
	kubeClient   kubernetes.Interface
	apolloClient *apollo.Client
}

var _ framework.ScorePlugin = &StorageTierAware{}
var _ framework.FilterPlugin = &StorageTierAware{}

// NewStorageTierAware creates a new StorageTierAware plugin
func NewStorageTierAware(cache *utils.Cache, kubeClient kubernetes.Interface) *StorageTierAware {
	return &StorageTierAware{
		cache:        cache,
		kubeClient:   kubeClient,
		apolloClient: apollo.GetClient(),
	}
}

func (s *StorageTierAware) Name() string {
	return StorageTierAwareName
}

// Filter filters out nodes that don't meet minimum storage tier requirements
func (s *StorageTierAware) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	node := nodeInfo.Node()
	if node == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	// Get scheduling policy from APOLLO
	policy := s.getSchedulingPolicy(pod)

	// Get required storage tier from APOLLO or pod annotations
	requiredTier := s.getRequiredStorageTier(policy, pod)
	if requiredTier == StorageTierUnknown {
		return utils.NewStatus(utils.Success, "") // No specific requirement
	}

	// Get node's available storage tiers
	nodeTiers := s.getNodeStorageTiers(node)
	if len(nodeTiers) == 0 {
		// Node has no storage tier labels - allow if not strict requirement
		if s.isStrictTierRequired(policy, pod) {
			return utils.NewStatus(utils.Unschedulable,
				"node %s has no storage tier labels, strict tier required", node.Name)
		}
		return utils.NewStatus(utils.Success, "")
	}

	// Check if required tier is available
	for _, tier := range nodeTiers {
		if tier >= requiredTier {
			return utils.NewStatus(utils.Success, "")
		}
	}

	if s.isStrictTierRequired(policy, pod) {
		return utils.NewStatus(utils.Unschedulable,
			"node %s does not have required storage tier %s", node.Name, requiredTier.String())
	}

	return utils.NewStatus(utils.Success, "")
}

// Score scores nodes based on storage tier match with workload requirements
func (s *StorageTierAware) Score(ctx context.Context, pod *v1.Pod, nodeName string) (int64, *utils.Status) {
	nodeInfo := s.cache.Nodes()[nodeName]
	if nodeInfo == nil {
		return 0, utils.NewStatus(utils.Error, "node not found in cache")
	}

	node := nodeInfo.Node()
	if node == nil {
		return 0, utils.NewStatus(utils.Error, "node not found")
	}

	// Get scheduling policy from APOLLO with source tracking
	policy, dataSource := s.getSchedulingPolicyWithSource(pod)

	score := int64(0)

	// APOLLO가 분석한 I/O 패턴 사용
	ioPatternScore, ioPatternSource := s.calculateIOPatternScoreWithSource(policy, pod, node, dataSource)
	score += ioPatternScore

	// 2. 파이프라인 단계별 최적 스토리지 매칭 (
	pipelineScore, pipelineSource := s.calculatePipelineStageScoreWithSource(policy, pod, node, dataSource)
	score += pipelineScore

	// 3. IOPS/처리량 요구사항 충족도 (0-20점)
	// APOLLO에서 받은 최소 요구사항 사용
	performanceScore, performanceSource := s.calculatePerformanceScoreWithSource(policy, pod, node, dataSource)
	score += performanceScore

	// 4. 스토리지 가용 용량 (0-10점) - 항상 Node annotations에서 가져옴
	capacityScore := s.calculateCapacityScore(pod, node)
	score += capacityScore

	logger.Info("[StorageTierAware] Node scored",
		"node", nodeName, "score", score,
		"ioPatternScore", ioPatternScore, "ioPatternSource", ioPatternSource,
		"pipelineScore", pipelineScore, "pipelineSource", pipelineSource,
		"performanceScore", performanceScore, "performanceSource", performanceSource,
		"capacityScore", capacityScore, "capacitySource", "Node-Annotations")

	return score, utils.NewStatus(utils.Success, "")
}

func (s *StorageTierAware) ScoreExtensions() framework.ScoreExtensions {
	return s
}

func (s *StorageTierAware) NormalizeScore(ctx context.Context, pod *v1.Pod, scores utils.PluginResult) *utils.Status {
	return utils.NewStatus(utils.Success, "")
}

// getSchedulingPolicyWithSource fetches scheduling policy from APOLLO with source tracking
func (s *StorageTierAware) getSchedulingPolicyWithSource(pod *v1.Pod) (*apollo.SchedulingPolicy, apollo.DataSource) {
	if s.apolloClient == nil {
		logger.Info("[StorageTierAware-DataSource] FALLBACK - No APOLLO client, using Pod labels/annotations",
			"namespace", pod.Namespace, "pod", pod.Name)
		return nil, apollo.DataSourceFallback
	}

	result := s.apolloClient.GetSchedulingPolicyWithSource(
		pod.Namespace,
		pod.Name,
		string(pod.UID),
		pod.Labels,
		pod.Annotations,
	)

	return result.Policy, result.Source
}

// getSchedulingPolicy fetches scheduling policy from APOLLO (legacy wrapper)
func (s *StorageTierAware) getSchedulingPolicy(pod *v1.Pod) *apollo.SchedulingPolicy {
	policy, _ := s.getSchedulingPolicyWithSource(pod)
	return policy
}

// getRequiredStorageTier extracts required storage tier from APOLLO policy or pod
func (s *StorageTierAware) getRequiredStorageTier(policy *apollo.SchedulingPolicy, pod *v1.Pod) StorageTier {
	// First check APOLLO policy
	if policy != nil {
		storageClass := apollo.GetStorageClass(policy)
		switch storageClass {
		case apollo.StorageClass_STORAGE_CLASS_ULTRA_FAST:
			return StorageTierNVMe
		case apollo.StorageClass_STORAGE_CLASS_FAST:
			return StorageTierSSD
		case apollo.StorageClass_STORAGE_CLASS_STANDARD:
			return StorageTierHDD
		case apollo.StorageClass_STORAGE_CLASS_CSD:
			return StorageTierCSD
		case apollo.StorageClass_STORAGE_CLASS_MEMORY:
			return StorageTierMemory
		}
	}

	// Fallback: Check pod annotation
	if tier, ok := pod.Annotations["ai-storage.keti/required-storage-tier"]; ok {
		return s.parseStorageTier(tier)
	}

	// Check label
	if tier, ok := pod.Labels["storage-tier"]; ok {
		return s.parseStorageTier(tier)
	}

	return StorageTierUnknown
}

// parseStorageTier converts string to StorageTier
func (s *StorageTierAware) parseStorageTier(tier string) StorageTier {
	switch strings.ToLower(tier) {
	case "nvme", "ultra-fast":
		return StorageTierNVMe
	case "ssd", "fast":
		return StorageTierSSD
	case "hdd", "standard":
		return StorageTierHDD
	case "csd", "computational":
		return StorageTierCSD
	case "memory", "ram":
		return StorageTierMemory
	default:
		return StorageTierUnknown
	}
}

// isStrictTierRequired checks if strict tier matching is required
func (s *StorageTierAware) isStrictTierRequired(policy *apollo.SchedulingPolicy, pod *v1.Pod) bool {
	// Check APOLLO decision
	if policy != nil && policy.Decision == apollo.SchedulingDecision_SCHEDULING_DECISION_REQUIRE {
		return true
	}

	// Fallback to pod annotation
	if strict, ok := pod.Annotations["ai-storage.keti/strict-storage-tier"]; ok {
		return strict == "true"
	}
	return false
}

// getNodeStorageTiers extracts available storage tiers from node
func (s *StorageTierAware) getNodeStorageTiers(node *v1.Node) []StorageTier {
	labels := node.Labels
	if labels == nil {
		return nil
	}

	var tiers []StorageTier

	// Check for specific tier labels
	if _, ok := labels["storage-tier/nvme"]; ok {
		tiers = append(tiers, StorageTierNVMe)
	}
	if _, ok := labels["storage-tier/ssd"]; ok {
		tiers = append(tiers, StorageTierSSD)
	}
	if _, ok := labels["storage-tier/hdd"]; ok {
		tiers = append(tiers, StorageTierHDD)
	}
	if _, ok := labels["storage-tier/csd"]; ok {
		tiers = append(tiers, StorageTierCSD)
	}
	if _, ok := labels["storage-tier/memory"]; ok {
		tiers = append(tiers, StorageTierMemory)
	}

	// Check for combined storage-tier label
	if tier, ok := labels["storage-tier"]; ok {
		parsedTier := s.parseStorageTier(tier)
		if parsedTier != StorageTierUnknown {
			exists := false
			for _, t := range tiers {
				if t == parsedTier {
					exists = true
					break
				}
			}
			if !exists {
				tiers = append(tiers, parsedTier)
			}
		}
	}

	return tiers
}

// getHighestStorageTier returns the highest tier available on a node
func (s *StorageTierAware) getHighestStorageTier(node *v1.Node) StorageTier {
	tiers := s.getNodeStorageTiers(node)
	if len(tiers) == 0 {
		return StorageTierUnknown
	}

	highest := StorageTierUnknown
	for _, tier := range tiers {
		if tier > highest {
			highest = tier
		}
	}
	return highest
}

// calculateIOPatternScoreWithSource scores based on I/O pattern and storage tier match with source tracking
func (s *StorageTierAware) calculateIOPatternScoreWithSource(policy *apollo.SchedulingPolicy, pod *v1.Pod, node *v1.Node, policySource apollo.DataSource) (int64, string) {
	// Get max score from CRD config (defaults applied by configmanager)
	cfg := configmanager.GetManager().GetStorageTierConfig()
	maxScore := int64(cfg.Scoring.IOPatternScoreMax)

	// Get tier scores from CRD config
	tiersCfg := configmanager.GetManager().GetStorageTiersConfig()

	highestTier := s.getHighestStorageTier(node)
	if highestTier == StorageTierUnknown {
		return maxScore / 2, "NEUTRAL (no tier info)"
	}

	// Get I/O pattern from APOLLO or fallback
	ioPattern := apollo.IOPattern_IO_PATTERN_UNKNOWN
	source := "FALLBACK"

	if policy != nil && policySource != apollo.DataSourceFallback {
		ioPattern = apollo.GetIOPattern(policy)
		if ioPattern != apollo.IOPattern_IO_PATTERN_UNKNOWN {
			source = "APOLLO-IOPattern"
		}
	}

	// Fallback to pod annotations if no APOLLO data
	if ioPattern == apollo.IOPattern_IO_PATTERN_UNKNOWN {
		if pattern, ok := pod.Annotations["ai-storage.keti/io-pattern"]; ok {
			source = "Pod-Annotations"
			switch strings.ToLower(pattern) {
			case "random":
				ioPattern = apollo.IOPattern_IO_PATTERN_RANDOM
			case "sequential":
				ioPattern = apollo.IOPattern_IO_PATTERN_SEQUENTIAL
			case "bursty":
				ioPattern = apollo.IOPattern_IO_PATTERN_BURSTY
			case "read-heavy", "readheavy":
				ioPattern = apollo.IOPattern_IO_PATTERN_READ_HEAVY
			case "write-heavy", "writeheavy":
				ioPattern = apollo.IOPattern_IO_PATTERN_WRITE_HEAVY
			case "balanced":
				ioPattern = apollo.IOPattern_IO_PATTERN_BALANCED
			}
		}
	}

	if ioPattern == apollo.IOPattern_IO_PATTERN_UNKNOWN {
		return maxScore / 2, "NEUTRAL (unknown pattern)"
	}

	// Helper to get tier score from config
	getTierScore := func(tier StorageTier, pattern string) int64 {
		switch tier {
		case StorageTierNVMe, StorageTierMemory:
			switch pattern {
			case "random":
				return int64(tiersCfg.NVMe.RandomScore)
			case "sequential":
				return int64(tiersCfg.NVMe.SequentialScore)
			case "readHeavy":
				return int64(tiersCfg.NVMe.ReadHeavyScore)
			case "writeHeavy":
				return int64(tiersCfg.NVMe.WriteHeavyScore)
			default:
				return maxScore
			}
		case StorageTierSSD:
			switch pattern {
			case "random":
				return int64(tiersCfg.SSD.RandomScore)
			case "sequential":
				return int64(tiersCfg.SSD.SequentialScore)
			case "readHeavy":
				return int64(tiersCfg.SSD.ReadHeavyScore)
			case "writeHeavy":
				return int64(tiersCfg.SSD.WriteHeavyScore)
			default:
				return maxScore * 3 / 4
			}
		case StorageTierHDD:
			switch pattern {
			case "random":
				return int64(tiersCfg.HDD.RandomScore)
			case "sequential":
				return int64(tiersCfg.HDD.SequentialScore)
			case "readHeavy":
				return int64(tiersCfg.HDD.ReadHeavyScore)
			case "writeHeavy":
				return int64(tiersCfg.HDD.WriteHeavyScore)
			default:
				return maxScore / 2
			}
		case StorageTierCSD:
			return maxScore / 2
		default:
			return maxScore / 4
		}
	}

	// I/O 패턴별 최적 스토리지 티어 매칭
	switch ioPattern {
	case apollo.IOPattern_IO_PATTERN_RANDOM:
		return getTierScore(highestTier, "random"), source
	case apollo.IOPattern_IO_PATTERN_SEQUENTIAL:
		return getTierScore(highestTier, "sequential"), source
	case apollo.IOPattern_IO_PATTERN_BURSTY:
		switch highestTier {
		case StorageTierNVMe, StorageTierMemory:
			return maxScore, source
		case StorageTierSSD:
			return maxScore * 5 / 8, source
		case StorageTierHDD:
			return maxScore / 4, source
		case StorageTierCSD:
			return maxScore / 2, source
		}
	case apollo.IOPattern_IO_PATTERN_READ_HEAVY:
		return getTierScore(highestTier, "readHeavy"), source
	case apollo.IOPattern_IO_PATTERN_WRITE_HEAVY:
		return getTierScore(highestTier, "writeHeavy"), source
	case apollo.IOPattern_IO_PATTERN_BALANCED:
		switch highestTier {
		case StorageTierNVMe, StorageTierMemory:
			return maxScore, source
		case StorageTierSSD:
			return maxScore * 3 / 4, source
		case StorageTierCSD:
			return maxScore / 2, source
		case StorageTierHDD:
			return maxScore * 3 / 8, source
		}
	}

	return maxScore / 2, source
}

// calculateIOPatternScore scores based on I/O pattern and storage tier match (legacy wrapper)
func (s *StorageTierAware) calculateIOPatternScore(policy *apollo.SchedulingPolicy, pod *v1.Pod, node *v1.Node) int64 {
	score, _ := s.calculateIOPatternScoreWithSource(policy, pod, node, apollo.DataSourceAPOLLO)
	return score
}

// calculatePipelineStageScoreWithSource scores based on pipeline stage requirements with source tracking
func (s *StorageTierAware) calculatePipelineStageScoreWithSource(policy *apollo.SchedulingPolicy, pod *v1.Pod, node *v1.Node, policySource apollo.DataSource) (int64, string) {
	// Get max score from CRD config (defaults applied by configmanager)
	cfg := configmanager.GetManager().GetStorageTierConfig()
	maxScore := int64(cfg.Scoring.PipelineStageScoreMax)

	highestTier := s.getHighestStorageTier(node)

	// Get pipeline step from APOLLO or fallback
	pipelineStep := ""
	source := "FALLBACK"

	if policy != nil && policySource != apollo.DataSourceFallback {
		pipelineStep = apollo.GetPipelineStep(policy)
		if pipelineStep != "" {
			source = "APOLLO-PipelineStep"
		}
	}
	if pipelineStep == "" {
		pipelineStep = s.getPipelineStageFromPod(pod)
		if pipelineStep != "" {
			source = "Pod-Labels/Annotations"
		}
	}

	if pipelineStep == "" {
		return maxScore / 2, "NEUTRAL (no pipeline info)"
	}

	hasCSD := false
	for _, tier := range s.getNodeStorageTiers(node) {
		if tier == StorageTierCSD {
			hasCSD = true
			break
		}
	}

	switch pipelineStep {
	case "preprocessing", "preprocess", "data-loading":
		if hasCSD {
			return maxScore, source
		}
		switch highestTier {
		case StorageTierNVMe:
			return maxScore * 5 / 6, source
		case StorageTierSSD:
			return maxScore * 5 / 6, source
		case StorageTierHDD:
			return maxScore * 2 / 3, source
		default:
			return maxScore / 2, source
		}

	case "training", "train":
		switch highestTier {
		case StorageTierNVMe, StorageTierMemory:
			return maxScore, source
		case StorageTierSSD:
			return maxScore * 5 / 6, source
		case StorageTierCSD:
			return maxScore * 2 / 3, source
		case StorageTierHDD:
			return maxScore / 3, source
		default:
			return maxScore / 2, source
		}

	case "evaluation", "eval", "validation":
		switch highestTier {
		case StorageTierNVMe, StorageTierMemory:
			return maxScore, source
		case StorageTierSSD:
			return maxScore * 9 / 10, source
		case StorageTierCSD:
			return maxScore * 2 / 3, source
		case StorageTierHDD:
			return maxScore / 2, source
		default:
			return maxScore / 2, source
		}

	case "serving", "inference":
		switch highestTier {
		case StorageTierNVMe, StorageTierMemory:
			return maxScore, source
		case StorageTierSSD:
			return maxScore * 2 / 3, source
		default:
			return maxScore / 3, source
		}
	}

	return maxScore / 2, source
}

// calculatePipelineStageScore scores based on pipeline stage requirements (legacy wrapper)
func (s *StorageTierAware) calculatePipelineStageScore(policy *apollo.SchedulingPolicy, pod *v1.Pod, node *v1.Node) int64 {
	score, _ := s.calculatePipelineStageScoreWithSource(policy, pod, node, apollo.DataSourceAPOLLO)
	return score
}

// getPipelineStageFromPod extracts pipeline stage from pod (fallback)
func (s *StorageTierAware) getPipelineStageFromPod(pod *v1.Pod) string {
	if stage, ok := pod.Annotations["ai-storage.keti/pipeline-stage"]; ok {
		return strings.ToLower(stage)
	}
	if stage, ok := pod.Labels["pipeline-step"]; ok {
		return strings.ToLower(stage)
	}
	if stage, ok := pod.Labels["stage"]; ok {
		return strings.ToLower(stage)
	}
	return ""
}

// calculatePerformanceScoreWithSource scores based on IOPS/throughput requirements with source tracking
func (s *StorageTierAware) calculatePerformanceScoreWithSource(policy *apollo.SchedulingPolicy, pod *v1.Pod, node *v1.Node, policySource apollo.DataSource) (int64, string) {
	// Get max score from CRD config (defaults applied by configmanager)
	cfg := configmanager.GetManager().GetStorageTierConfig()
	maxScore := int64(cfg.Scoring.IOPSScoreMax)

	// Get required IOPS/throughput from APOLLO or fallback
	requiredIOPS := int64(0)
	requiredThroughput := int64(0)
	iopsSource := "NEUTRAL"
	throughputSource := "NEUTRAL"

	if policy != nil && policySource != apollo.DataSourceFallback {
		requiredIOPS = apollo.GetMinIOPS(policy)
		requiredThroughput = apollo.GetMinThroughput(policy)
		if requiredIOPS > 0 {
			iopsSource = "APOLLO-MinIOPS"
		}
		if requiredThroughput > 0 {
			throughputSource = "APOLLO-MinThroughput"
		}
	}

	// Fallback to pod annotations
	if requiredIOPS == 0 {
		requiredIOPS = s.getRequiredIOPSFromPod(pod)
		if requiredIOPS > 0 {
			iopsSource = "Pod-Annotations"
		}
	}
	if requiredThroughput == 0 {
		requiredThroughput = s.getRequiredThroughputFromPod(pod)
		if requiredThroughput > 0 {
			throughputSource = "Pod-Annotations"
		}
	}

	// Determine primary source
	source := iopsSource
	if source == "NEUTRAL" {
		source = throughputSource
	}
	if source == "NEUTRAL" {
		source = "Node-Annotations (tier estimate)"
	}

	// Get node's storage performance
	nodeIOPS := s.getNodeIOPS(node)
	nodeThroughput := s.getNodeThroughput(node)

	score := maxScore / 2 // 기본 점수

	// IOPS 요구사항 충족 여부
	if requiredIOPS > 0 && nodeIOPS > 0 {
		if nodeIOPS >= requiredIOPS {
			ratio := float64(nodeIOPS) / float64(requiredIOPS)
			if ratio >= 2.0 {
				score += maxScore / 4 // 충분한 여유
			} else if ratio >= 1.0 {
				score += maxScore / 6 // 요구사항 충족
			}
		}
	} else {
		score += maxScore / 10 // 정보 없으면 중립
	}

	// 처리량 요구사항 충족 여부
	if requiredThroughput > 0 && nodeThroughput > 0 {
		if nodeThroughput >= requiredThroughput {
			ratio := float64(nodeThroughput) / float64(requiredThroughput)
			if ratio >= 2.0 {
				score += maxScore / 4
			} else if ratio >= 1.0 {
				score += maxScore / 6
			}
		}
	} else {
		score += maxScore / 10
	}

	if score > maxScore {
		score = maxScore
	}

	return score, source
}

// calculatePerformanceScore scores based on IOPS/throughput requirements (legacy wrapper)
func (s *StorageTierAware) calculatePerformanceScore(policy *apollo.SchedulingPolicy, pod *v1.Pod, node *v1.Node) int64 {
	score, _ := s.calculatePerformanceScoreWithSource(policy, pod, node, apollo.DataSourceAPOLLO)
	return score
}

// getRequiredIOPSFromPod extracts required IOPS from pod (fallback)
func (s *StorageTierAware) getRequiredIOPSFromPod(pod *v1.Pod) int64 {
	if iops, ok := pod.Annotations["ai-storage.keti/required-iops"]; ok {
		if val, err := strconv.ParseInt(iops, 10, 64); err == nil {
			return val
		}
	}
	return 0
}

// getRequiredThroughputFromPod extracts required throughput from pod (fallback)
func (s *StorageTierAware) getRequiredThroughputFromPod(pod *v1.Pod) int64 {
	if tp, ok := pod.Annotations["ai-storage.keti/required-throughput-mbps"]; ok {
		if val, err := strconv.ParseInt(tp, 10, 64); err == nil {
			return val
		}
	}
	return 0
}

// getNodeIOPS extracts available IOPS from node
func (s *StorageTierAware) getNodeIOPS(node *v1.Node) int64 {
	annotations := node.Annotations
	if annotations != nil {
		if iops, ok := annotations["ai-storage.keti/available-iops"]; ok {
			if val, err := strconv.ParseInt(iops, 10, 64); err == nil {
				return val
			}
		}
	}

	// Fallback: estimate from storage tier
	highestTier := s.getHighestStorageTier(node)
	switch highestTier {
	case StorageTierNVMe:
		return 100000
	case StorageTierSSD:
		return 10000
	case StorageTierHDD:
		return 150
	default:
		return 0
	}
}

// getNodeThroughput extracts available throughput from node
func (s *StorageTierAware) getNodeThroughput(node *v1.Node) int64 {
	annotations := node.Annotations
	if annotations != nil {
		if tp, ok := annotations["ai-storage.keti/available-throughput-mbps"]; ok {
			if val, err := strconv.ParseInt(tp, 10, 64); err == nil {
				return val
			}
		}
	}

	// Fallback: estimate from storage tier
	highestTier := s.getHighestStorageTier(node)
	switch highestTier {
	case StorageTierNVMe:
		return 3000
	case StorageTierSSD:
		return 500
	case StorageTierHDD:
		return 150
	default:
		return 0
	}
}

// calculateCapacityScore scores based on available storage capacity
func (s *StorageTierAware) calculateCapacityScore(pod *v1.Pod, node *v1.Node) int64 {
	// Get max score from CRD config (defaults applied by configmanager)
	cfg := configmanager.GetManager().GetStorageTierConfig()
	maxScore := int64(cfg.Scoring.CapacityScoreMax)

	annotations := node.Annotations
	if annotations == nil {
		return maxScore / 2 // 기본 점수
	}

	// Get available capacity (in GB)
	availableCapacity := int64(0)
	if cap, ok := annotations["ai-storage.keti/available-capacity-gb"]; ok {
		if val, err := strconv.ParseInt(cap, 10, 64); err == nil {
			availableCapacity = val
		}
	}

	// Get required capacity from pod
	requiredCapacity := s.getRequiredCapacity(pod)

	if availableCapacity == 0 {
		return maxScore / 2 // 정보 없음
	}

	if requiredCapacity > 0 {
		if availableCapacity >= requiredCapacity*2 {
			return maxScore // 충분한 여유
		} else if availableCapacity >= requiredCapacity {
			return maxScore * 7 / 10 // 요구사항 충족
		} else {
			return maxScore * 3 / 10 // 부족할 수 있음
		}
	}

	// 용량 정보만 있는 경우 - 높은 용량에 가산점
	if availableCapacity >= 1000 {
		return maxScore
	} else if availableCapacity >= 500 {
		return maxScore * 8 / 10
	} else if availableCapacity >= 100 {
		return maxScore * 6 / 10
	}

	return maxScore / 2
}

// getRequiredCapacity extracts required storage capacity (GB) from pod
func (s *StorageTierAware) getRequiredCapacity(pod *v1.Pod) int64 {
	if cap, ok := pod.Annotations["ai-storage.keti/required-capacity-gb"]; ok {
		if val, err := strconv.ParseInt(cap, 10, 64); err == nil {
			return val
		}
	}
	return 0
}
