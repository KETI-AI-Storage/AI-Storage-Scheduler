// ============================================
// IOPatternBased Plugin
// 전처리 유형별 (증강/변환/필터링) 최적화
// ============================================
//
// 이 플러그인은 APOLLO로부터 워크로드 분석 결과를 받아
// 전처리 워크로드의 구체적인 유형에 최적화된 노드에 스케줄링합니다.
//
// APOLLO 연동:
// - insight-trace가 분석한 전처리 유형 정보 활용
// - 워크로드 시그니처 기반 I/O 특성 파악
// - APOLLO의 CSD 요구사항 확인
//
// 전처리 유형:
// 1. Augmentation (증강)
//    - 이미지 회전, 크롭, 색상 변환 등
//    - CPU 집약적, 메모리 대역폭 중요
//    - 읽기 1 : 쓰기 N 패턴 (데이터 증가)
//
// 2. Transformation (변환)
//    - 포맷 변환, 리샘플링, 정규화
//    - 균형 잡힌 I/O, CPU 사용
//    - 읽기 1 : 쓰기 1 패턴 (데이터 크기 유지)
//
// 3. Filtering (필터링)
//    - 데이터 정제, 이상치 제거, 샘플링
//    - 읽기 집약적, 쓰기는 적음
//    - 읽기 N : 쓰기 1 패턴 (데이터 감소)
//
// 4. Aggregation (집계)
//    - 통계 계산, 특성 추출
//    - 읽기 많음, 결과는 작음
//    - 메모리 중요
//
// 5. Sharding (샤딩/파티셔닝)
//    - 데이터 분할, 배포 준비
//    - 순차 읽기, 다중 순차 쓰기
//    - 디스크 I/O 집약적
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

const IOPatternBasedName = "IOPatternBased"

// IOCharacteristics describes the I/O behavior of a preprocessing type
type IOCharacteristics struct {
	ReadIntensive    bool    // 읽기 집약적
	WriteIntensive   bool    // 쓰기 집약적
	CPUIntensive     bool    // CPU 집약적
	MemoryIntensive  bool    // 메모리 집약적
	DataExpansion    float64 // 데이터 크기 변화 비율 (1.0 = 동일, >1 = 증가, <1 = 감소)
	SequentialAccess bool    // 순차 접근 선호
	RequiresCSD      bool    // CSD 오프로드 가능
}

// IOPatternBased scores nodes based on preprocessing workload type
type IOPatternBased struct {
	cache        *utils.Cache
	kubeClient   kubernetes.Interface
	apolloClient *apollo.Client
}

var _ framework.ScorePlugin = &IOPatternBased{}
var _ framework.FilterPlugin = &IOPatternBased{}

// NewIOPatternBased creates a new IOPatternBased plugin
func NewIOPatternBased(cache *utils.Cache, kubeClient kubernetes.Interface) *IOPatternBased {
	return &IOPatternBased{
		cache:        cache,
		kubeClient:   kubeClient,
		apolloClient: apollo.GetClient(),
	}
}

func (p *IOPatternBased) Name() string {
	return IOPatternBasedName
}

// Filter filters out nodes that cannot handle the preprocessing workload type
func (p *IOPatternBased) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	node := nodeInfo.Node()
	if node == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	// Get scheduling policy from APOLLO
	policy := p.getSchedulingPolicy(pod)

	// 전처리 워크로드가 아니면 필터링 안함
	if !apollo.IsPreprocessingWorkload(policy) && !p.isPreprocessingWorkloadFromPod(pod) {
		return utils.NewStatus(utils.Success, "")
	}

	// Get I/O characteristics from APOLLO or local analysis
	characteristics := p.getIOCharacteristicsFromPolicy(policy, pod)

	// CSD가 필요한 워크로드인데 CSD가 없는 노드는 필터링
	// APOLLO에서 CSD 요구사항 확인
	requiresCSD := apollo.RequiresCSD(policy) || characteristics.RequiresCSD
	if requiresCSD && !p.hasCSD(node) {
		if p.isCSDRequired(pod) {
			return utils.NewStatus(utils.Unschedulable,
				"workload requires CSD but node %s has no CSD", node.Name)
		}
	}

	return utils.NewStatus(utils.Success, "")
}

// Score scores nodes based on preprocessing workload type match
func (p *IOPatternBased) Score(ctx context.Context, pod *v1.Pod, nodeName string) (int64, *utils.Status) {
	nodeInfo := p.cache.Nodes()[nodeName]
	if nodeInfo == nil {
		return 0, utils.NewStatus(utils.Error, "node not found in cache")
	}
	node := nodeInfo.Node()
	if node == nil {
		return 0, utils.NewStatus(utils.Error, "node not found")
	}
	policy, dataSource := p.getSchedulingPolicyWithSource(pod)

	isPreprocessFromAPOLLO := apollo.IsPreprocessingWorkload(policy)
	isPreprocessFromPod := p.isPreprocessingWorkloadFromPod(pod)

	if !isPreprocessFromAPOLLO && !isPreprocessFromPod {
		return 50, utils.NewStatus(utils.Success, "")
	}
	if isPreprocessFromAPOLLO {
		logger.Info("[IOPatternBased-DataSource] Preprocessing detected from APOLLO",
			"namespace", pod.Namespace, "pod", pod.Name, "source", dataSource)
	} else if isPreprocessFromPod {
		logger.Info("[IOPatternBased-DataSource] Preprocessing detected from Pod labels/annotations (FALLBACK)",
			"namespace", pod.Namespace, "pod", pod.Name)
	}

	// Get preprocessing type and I/O characteristics from APOLLO with source tracking
	preprocessType, preprocessTypeSource := p.getPreprocessingTypeWithSource(policy, pod, dataSource)
	characteristics, charsSource := p.getIOCharacteristicsWithSource(policy, pod, dataSource)

	score := int64(0)

	// 0. APOLLO 노드 선호도 점수 (0-15점)
	apolloScore, apolloScoreSource := p.calculateAPOLLOPreferenceScoreWithSource(policy, nodeName, dataSource)
	score += apolloScore

	// 1. 전처리 유형별 리소스 매칭 (0-25점) - Node labels/annotations에서 가져옴
	resourceScore := p.calculateResourceMatchScore(characteristics, node)
	score += resourceScore

	// 2. I/O 패턴 최적화 점수 (0-20점) - Node labels/annotations에서 가져옴
	ioScore := p.calculateIOOptimizationScore(characteristics, node)
	score += ioScore

	// 3. 데이터 확장/축소 대응 점수 (0-20점) - Node annotations에서 가져옴
	expansionScore := p.calculateExpansionScore(characteristics, node)
	score += expansionScore

	// 4. CSD 오프로드 가능성 점수 (0-20점)
	csdScore, csdScoreSource := p.calculateCSDScoreWithSource(policy, characteristics, node, dataSource)
	score += csdScore

	logger.Info("[IOPatternBased] Node scored",
		"node", nodeName, "score", score,
		"preprocessType", preprocessType.String(), "preprocessTypeSource", preprocessTypeSource,
		"charsSource", charsSource,
		"apolloScore", apolloScore, "apolloScoreSource", apolloScoreSource,
		"resourceScore", resourceScore, "resourceScoreSource", "Node-Labels",
		"ioScore", ioScore, "ioScoreSource", "Node-Labels",
		"expansionScore", expansionScore, "expansionScoreSource", "Node-Annotations",
		"csdScore", csdScore, "csdScoreSource", csdScoreSource)

	return score, utils.NewStatus(utils.Success, "")
}

func (p *IOPatternBased) ScoreExtensions() framework.ScoreExtensions {
	return p
}

func (p *IOPatternBased) NormalizeScore(ctx context.Context, pod *v1.Pod, scores utils.PluginResult) *utils.Status {
	return utils.NewStatus(utils.Success, "")
}

// getSchedulingPolicyWithSource fetches scheduling policy from APOLLO with source tracking
func (p *IOPatternBased) getSchedulingPolicyWithSource(pod *v1.Pod) (*apollo.SchedulingPolicy, apollo.DataSource) {
	if p.apolloClient == nil {
		logger.Info("[IOPatternBased-DataSource] FALLBACK - No APOLLO client, using Pod labels/annotations",
			"namespace", pod.Namespace, "pod", pod.Name)
		return nil, apollo.DataSourceFallback
	}

	result := p.apolloClient.GetSchedulingPolicyWithSource(
		pod.Namespace,
		pod.Name,
		string(pod.UID),
		pod.Labels,
		pod.Annotations,
	)

	return result.Policy, result.Source
}

// getSchedulingPolicy fetches scheduling policy from APOLLO (legacy wrapper)
func (p *IOPatternBased) getSchedulingPolicy(pod *v1.Pod) *apollo.SchedulingPolicy {
	policy, _ := p.getSchedulingPolicyWithSource(pod)
	return policy
}

// calculateAPOLLOPreferenceScoreWithSource calculates score based on APOLLO's node preference with source tracking
func (p *IOPatternBased) calculateAPOLLOPreferenceScoreWithSource(policy *apollo.SchedulingPolicy, nodeName string, policySource apollo.DataSource) (int64, string) {
	// Get max score from CRD config (defaults applied by configmanager)
	cfg := configmanager.GetManager().GetIOPatternConfig()
	maxScore := int64(cfg.Scoring.ApolloPreferenceScoreMax)

	if policy == nil || policySource == apollo.DataSourceFallback {
		return maxScore / 2, "NEUTRAL (no APOLLO data)"
	}

	prefScore := apollo.GetNodePreferenceScore(policy, nodeName)
	if prefScore > 0 {
		// APOLLO 점수를 0-maxScore 범위로 매핑 (APOLLO는 0-100)
		return int64(prefScore) * maxScore / 100, "APOLLO-NodePreference"
	}

	return maxScore / 3, "APOLLO-Default"
}

// calculateAPOLLOPreferenceScore calculates score based on APOLLO's node preference (legacy wrapper)
func (p *IOPatternBased) calculateAPOLLOPreferenceScore(policy *apollo.SchedulingPolicy, nodeName string) int64 {
	score, _ := p.calculateAPOLLOPreferenceScoreWithSource(policy, nodeName, apollo.DataSourceAPOLLO)
	return score
}

// getPreprocessingTypeWithSource gets preprocessing type from APOLLO policy with source tracking
func (p *IOPatternBased) getPreprocessingTypeWithSource(policy *apollo.SchedulingPolicy, pod *v1.Pod, policySource apollo.DataSource) (apollo.PreprocessingType, string) {
	// APOLLO에서 전처리 유형 가져오기
	if policy != nil && policySource != apollo.DataSourceFallback {
		preprocessType := apollo.GetPreprocessingType(policy)
		if preprocessType != apollo.PreprocessingType_PREPROCESSING_TYPE_UNKNOWN {
			return preprocessType, "APOLLO-PreprocessingType"
		}
	}

	// Fallback: Pod 어노테이션/라벨에서 확인
	if ptype, ok := pod.Annotations["ai-storage.keti/preprocessing-type"]; ok {
		return p.parsePreprocessingType(ptype), "Pod-Annotations"
	}
	if ptype, ok := pod.Labels["preprocessing-type"]; ok {
		return p.parsePreprocessingType(ptype), "Pod-Labels"
	}

	return apollo.PreprocessingType_PREPROCESSING_TYPE_UNKNOWN, "UNKNOWN"
}

// getPreprocessingTypeFromPolicy gets preprocessing type from APOLLO policy (legacy wrapper)
func (p *IOPatternBased) getPreprocessingTypeFromPolicy(policy *apollo.SchedulingPolicy, pod *v1.Pod) apollo.PreprocessingType {
	ptype, _ := p.getPreprocessingTypeWithSource(policy, pod, apollo.DataSourceAPOLLO)
	return ptype
}

// parsePreprocessingType converts string to PreprocessingType
func (p *IOPatternBased) parsePreprocessingType(ptype string) apollo.PreprocessingType {
	switch strings.ToLower(ptype) {
	case "augmentation", "augment", "aug":
		return apollo.PreprocessingType_PREPROCESSING_TYPE_AUGMENTATION
	case "transformation", "transform", "convert":
		return apollo.PreprocessingType_PREPROCESSING_TYPE_TRANSFORMATION
	case "filtering", "filter", "clean":
		return apollo.PreprocessingType_PREPROCESSING_TYPE_FILTERING
	case "aggregation", "aggregate", "agg":
		return apollo.PreprocessingType_PREPROCESSING_TYPE_AGGREGATION
	case "sharding", "shard", "partition":
		return apollo.PreprocessingType_PREPROCESSING_TYPE_SHARDING
	case "feature_extraction", "feature", "extract":
		return apollo.PreprocessingType_PREPROCESSING_TYPE_FEATURE_EXTRACT
	case "normalization", "normalize", "norm":
		return apollo.PreprocessingType_PREPROCESSING_TYPE_NORMALIZATION
	default:
		return apollo.PreprocessingType_PREPROCESSING_TYPE_UNKNOWN
	}
}

// getIOCharacteristicsWithSource gets I/O characteristics from APOLLO policy with source tracking
func (p *IOPatternBased) getIOCharacteristicsWithSource(policy *apollo.SchedulingPolicy, pod *v1.Pod, policySource apollo.DataSource) (IOCharacteristics, string) {
	chars, _ := p.getIOCharacteristicsFromPolicyInternal(policy, pod, policySource)
	source := "FALLBACK"

	if policy != nil && policySource != apollo.DataSourceFallback {
		preprocessType := apollo.GetPreprocessingType(policy)
		if preprocessType != apollo.PreprocessingType_PREPROCESSING_TYPE_UNKNOWN {
			source = "APOLLO-PreprocessingType"
		} else {
			ioPattern := apollo.GetIOPattern(policy)
			if ioPattern != apollo.IOPattern_IO_PATTERN_UNKNOWN {
				source = "APOLLO-IOPattern"
			}
		}
	}

	if source == "FALLBACK" {
		// Check if got from CRD config
		if _, ok := pod.Annotations["ai-storage.keti/preprocessing-type"]; ok {
			source = "CRD-Config (via Pod annotation)"
		} else if _, ok := pod.Labels["preprocessing-type"]; ok {
			source = "CRD-Config (via Pod label)"
		} else {
			source = "Default"
		}
	}

	return chars, source
}

// getIOCharacteristicsFromPolicyInternal is the internal implementation
func (p *IOPatternBased) getIOCharacteristicsFromPolicyInternal(policy *apollo.SchedulingPolicy, pod *v1.Pod, policySource apollo.DataSource) (IOCharacteristics, string) {
	// APOLLO에서 전처리 유형 확인
	preprocessType := apollo.PreprocessingType_PREPROCESSING_TYPE_UNKNOWN
	ioPattern := apollo.IOPattern_IO_PATTERN_UNKNOWN
	requiresCSD := false

	if policy != nil && policySource != apollo.DataSourceFallback {
		preprocessType = apollo.GetPreprocessingType(policy)
		ioPattern = apollo.GetIOPattern(policy)
		requiresCSD = apollo.RequiresCSD(policy)
	}

	// Try to get characteristics from CRD config
	typeName := p.preprocessTypeToString(preprocessType)
	if typeName != "" {
		if cfg, ok := configmanager.GetManager().GetPreprocessingTypeConfig(typeName); ok {
			return IOCharacteristics{
				ReadIntensive:    cfg.ReadIntensive,
				WriteIntensive:   cfg.WriteIntensive,
				CPUIntensive:     cfg.CPUIntensive,
				MemoryIntensive:  cfg.MemoryIntensive,
				DataExpansion:    cfg.DataExpansion,
				SequentialAccess: cfg.SequentialAccess,
				RequiresCSD:      requiresCSD || cfg.RequiresCSD,
			}, "CRD-Config"
		}
	}

	// Fallback to hardcoded defaults if not in CRD
	switch preprocessType {
	case apollo.PreprocessingType_PREPROCESSING_TYPE_AUGMENTATION:
		return IOCharacteristics{
			ReadIntensive:    true,
			WriteIntensive:   true,
			CPUIntensive:     true,
			MemoryIntensive:  true,
			DataExpansion:    5.0,
			SequentialAccess: true,
			RequiresCSD:      requiresCSD,
		}, "Hardcoded-Default"

	case apollo.PreprocessingType_PREPROCESSING_TYPE_TRANSFORMATION:
		return IOCharacteristics{
			ReadIntensive:    true,
			WriteIntensive:   true,
			CPUIntensive:     true,
			MemoryIntensive:  false,
			DataExpansion:    1.0,
			SequentialAccess: true,
			RequiresCSD:      requiresCSD || true,
		}, "Hardcoded-Default"

	case apollo.PreprocessingType_PREPROCESSING_TYPE_FILTERING:
		return IOCharacteristics{
			ReadIntensive:    true,
			WriteIntensive:   false,
			CPUIntensive:     false,
			MemoryIntensive:  false,
			DataExpansion:    0.3,
			SequentialAccess: true,
			RequiresCSD:      requiresCSD || true,
		}, "Hardcoded-Default"

	case apollo.PreprocessingType_PREPROCESSING_TYPE_AGGREGATION:
		return IOCharacteristics{
			ReadIntensive:    true,
			WriteIntensive:   false,
			CPUIntensive:     true,
			MemoryIntensive:  true,
			DataExpansion:    0.01,
			SequentialAccess: true,
			RequiresCSD:      requiresCSD || true,
		}, "Hardcoded-Default"

	case apollo.PreprocessingType_PREPROCESSING_TYPE_SHARDING:
		return IOCharacteristics{
			ReadIntensive:    true,
			WriteIntensive:   true,
			CPUIntensive:     false,
			MemoryIntensive:  false,
			DataExpansion:    1.0,
			SequentialAccess: true,
			RequiresCSD:      requiresCSD,
		}, "Hardcoded-Default"

	case apollo.PreprocessingType_PREPROCESSING_TYPE_FEATURE_EXTRACT:
		return IOCharacteristics{
			ReadIntensive:    true,
			WriteIntensive:   false,
			CPUIntensive:     true,
			MemoryIntensive:  true,
			DataExpansion:    0.1,
			SequentialAccess: true,
			RequiresCSD:      requiresCSD || true,
		}, "Hardcoded-Default"

	case apollo.PreprocessingType_PREPROCESSING_TYPE_NORMALIZATION:
		return IOCharacteristics{
			ReadIntensive:    true,
			WriteIntensive:   true,
			CPUIntensive:     true,
			MemoryIntensive:  false,
			DataExpansion:    1.0,
			SequentialAccess: true,
			RequiresCSD:      requiresCSD || true,
		}, "Hardcoded-Default"
	}

	// APOLLO에서 I/O 패턴 정보로 특성 추론
	if ioPattern != apollo.IOPattern_IO_PATTERN_UNKNOWN {
		return p.inferCharacteristicsFromIOPattern(ioPattern, requiresCSD), "Inferred-from-IOPattern"
	}

	// 기본값
	return IOCharacteristics{
		ReadIntensive:    true,
		WriteIntensive:   false,
		CPUIntensive:     false,
		MemoryIntensive:  false,
		DataExpansion:    1.0,
		SequentialAccess: true,
		RequiresCSD:      requiresCSD,
	}, "Default"
}

// getIOCharacteristicsFromPolicy gets I/O characteristics from APOLLO policy (legacy wrapper)
func (p *IOPatternBased) getIOCharacteristicsFromPolicy(policy *apollo.SchedulingPolicy, pod *v1.Pod) IOCharacteristics {
	chars, _ := p.getIOCharacteristicsFromPolicyInternal(policy, pod, apollo.DataSourceAPOLLO)
	return chars
}

// preprocessTypeToString converts PreprocessingType to CRD config key
func (p *IOPatternBased) preprocessTypeToString(ptype apollo.PreprocessingType) string {
	switch ptype {
	case apollo.PreprocessingType_PREPROCESSING_TYPE_AUGMENTATION:
		return "augmentation"
	case apollo.PreprocessingType_PREPROCESSING_TYPE_TRANSFORMATION:
		return "transformation"
	case apollo.PreprocessingType_PREPROCESSING_TYPE_FILTERING:
		return "filtering"
	case apollo.PreprocessingType_PREPROCESSING_TYPE_AGGREGATION:
		return "aggregation"
	case apollo.PreprocessingType_PREPROCESSING_TYPE_SHARDING:
		return "sharding"
	case apollo.PreprocessingType_PREPROCESSING_TYPE_FEATURE_EXTRACT:
		return "featureExtraction"
	case apollo.PreprocessingType_PREPROCESSING_TYPE_NORMALIZATION:
		return "normalization"
	default:
		return ""
	}
}

// inferCharacteristicsFromIOPattern infers I/O characteristics from I/O pattern
func (p *IOPatternBased) inferCharacteristicsFromIOPattern(ioPattern apollo.IOPattern, requiresCSD bool) IOCharacteristics {
	switch ioPattern {
	case apollo.IOPattern_IO_PATTERN_READ_HEAVY:
		return IOCharacteristics{
			ReadIntensive:    true,
			WriteIntensive:   false,
			CPUIntensive:     false,
			MemoryIntensive:  false,
			DataExpansion:    0.5,
			SequentialAccess: true,
			RequiresCSD:      requiresCSD,
		}
	case apollo.IOPattern_IO_PATTERN_WRITE_HEAVY:
		return IOCharacteristics{
			ReadIntensive:    false,
			WriteIntensive:   true,
			CPUIntensive:     false,
			MemoryIntensive:  false,
			DataExpansion:    2.0,
			SequentialAccess: true,
			RequiresCSD:      requiresCSD,
		}
	case apollo.IOPattern_IO_PATTERN_SEQUENTIAL:
		return IOCharacteristics{
			ReadIntensive:    true,
			WriteIntensive:   true,
			CPUIntensive:     false,
			MemoryIntensive:  false,
			DataExpansion:    1.0,
			SequentialAccess: true,
			RequiresCSD:      requiresCSD,
		}
	case apollo.IOPattern_IO_PATTERN_RANDOM:
		return IOCharacteristics{
			ReadIntensive:    true,
			WriteIntensive:   true,
			CPUIntensive:     false,
			MemoryIntensive:  true,
			DataExpansion:    1.0,
			SequentialAccess: false,
			RequiresCSD:      requiresCSD,
		}
	case apollo.IOPattern_IO_PATTERN_BALANCED:
		return IOCharacteristics{
			ReadIntensive:    true,
			WriteIntensive:   true,
			CPUIntensive:     true,
			MemoryIntensive:  true,
			DataExpansion:    1.0,
			SequentialAccess: true,
			RequiresCSD:      requiresCSD,
		}
	case apollo.IOPattern_IO_PATTERN_BURSTY:
		return IOCharacteristics{
			ReadIntensive:    true,
			WriteIntensive:   true,
			CPUIntensive:     false,
			MemoryIntensive:  true,
			DataExpansion:    1.0,
			SequentialAccess: false,
			RequiresCSD:      requiresCSD,
		}
	default:
		return IOCharacteristics{
			ReadIntensive:    true,
			WriteIntensive:   false,
			CPUIntensive:     false,
			MemoryIntensive:  false,
			DataExpansion:    1.0,
			SequentialAccess: true,
			RequiresCSD:      requiresCSD,
		}
	}
}

// isPreprocessingWorkloadFromPod checks pod labels/annotations directly (fallback)
func (p *IOPatternBased) isPreprocessingWorkloadFromPod(pod *v1.Pod) bool {
	// Check labels
	if stage, ok := pod.Labels["pipeline-step"]; ok {
		stage = strings.ToLower(stage)
		if stage == "preprocess" || stage == "preprocessing" || stage == "data-loading" {
			return true
		}
	}

	if stage, ok := pod.Labels["stage"]; ok {
		stage = strings.ToLower(stage)
		if stage == "preprocess" || stage == "preprocessing" || stage == "data-loading" {
			return true
		}
	}

	// Check annotation
	if wtype, ok := pod.Annotations["ai-storage.keti/workload-stage"]; ok {
		if strings.Contains(strings.ToLower(wtype), "preprocess") {
			return true
		}
	}

	// Check preprocessing type annotation
	if _, ok := pod.Annotations["ai-storage.keti/preprocessing-type"]; ok {
		return true
	}

	return false
}

// calculateResourceMatchScore scores based on resource requirements match
func (p *IOPatternBased) calculateResourceMatchScore(chars IOCharacteristics, node *v1.Node) int64 {
	// Get max score from CRD config (defaults applied by configmanager)
	cfg := configmanager.GetManager().GetIOPatternConfig()
	maxScore := int64(cfg.Scoring.ResourceMatchScoreMax)

	score := int64(0)
	labels := node.Labels
	annotations := node.Annotations

	// CPU 집약적 워크로드
	if chars.CPUIntensive {
		// 고성능 CPU 노드 확인
		if labels != nil {
			if _, ok := labels["node-capability/high-cpu"]; ok {
				score += maxScore * 8 / 25
			}
			if cpuCores, ok := labels["cpu-cores"]; ok {
				if cores, err := strconv.Atoi(cpuCores); err == nil && cores >= 16 {
					score += maxScore * 4 / 25
				}
			}
		}
	} else {
		score += maxScore * 4 / 25 // CPU 요구사항 낮음
	}

	// 메모리 집약적 워크로드
	if chars.MemoryIntensive {
		if labels != nil {
			if _, ok := labels["node-capability/high-memory"]; ok {
				score += maxScore * 8 / 25
			}
		}
		if annotations != nil {
			if memGB, ok := annotations["ai-storage.keti/available-memory-gb"]; ok {
				if mem, err := strconv.Atoi(memGB); err == nil && mem >= 64 {
					score += maxScore * 4 / 25
				}
			}
		}
	} else {
		score += maxScore * 4 / 25 // 메모리 요구사항 낮음
	}

	// 읽기 집약적 워크로드
	if chars.ReadIntensive {
		// 빠른 읽기 성능 노드
		if labels != nil {
			if _, ok := labels["storage-tier/nvme"]; ok {
				score += maxScore * 5 / 25
			} else if _, ok := labels["storage-tier/ssd"]; ok {
				score += maxScore * 3 / 25
			}
		}
	}

	if score > maxScore {
		score = maxScore
	}

	return score
}

// calculateIOOptimizationScore scores based on I/O pattern optimization
func (p *IOPatternBased) calculateIOOptimizationScore(chars IOCharacteristics, node *v1.Node) int64 {
	// Get max score from CRD config (defaults applied by configmanager)
	cfg := configmanager.GetManager().GetIOPatternConfig()
	maxScore := int64(cfg.Scoring.IOOptimizationScoreMax)

	score := int64(0)
	labels := node.Labels
	annotations := node.Annotations

	// 순차 접근 선호 워크로드
	if chars.SequentialAccess {
		// 순차 I/O에 최적화된 노드 (HDD도 괜찮음)
		score += maxScore * 8 / 20

		// 대용량 버퍼/캐시가 있는 노드 선호
		if annotations != nil {
			if _, ok := annotations["ai-storage.keti/large-io-buffer"]; ok {
				score += maxScore * 4 / 20
			}
		}
	}

	// 읽기와 쓰기 비율에 따른 최적화
	if chars.ReadIntensive && !chars.WriteIntensive {
		// 읽기 집약적 - 읽기 캐시가 있는 노드 선호
		if labels != nil {
			if _, ok := labels["storage-capability/read-cache"]; ok {
				score += maxScore * 4 / 20
			}
		}
	}

	if chars.WriteIntensive {
		// 쓰기 집약적 - 쓰기 버퍼가 있는 노드 선호
		if labels != nil {
			if _, ok := labels["storage-capability/write-buffer"]; ok {
				score += maxScore * 4 / 20
			}
			// NVMe가 버스트 쓰기에 좋음
			if _, ok := labels["storage-tier/nvme"]; ok {
				score += maxScore * 4 / 20
			}
		}
	}

	if score > maxScore {
		score = maxScore
	}

	return score
}

// calculateExpansionScore scores based on data expansion/reduction handling
func (p *IOPatternBased) calculateExpansionScore(chars IOCharacteristics, node *v1.Node) int64 {
	// Get max score from CRD config (defaults applied by configmanager)
	cfg := configmanager.GetManager().GetIOPatternConfig()
	maxScore := int64(cfg.Scoring.ExpansionScoreMax)

	score := maxScore / 2 // 기본 점수
	annotations := node.Annotations

	if chars.DataExpansion > 1.0 {
		// 데이터 증가 - 충분한 스토리지 용량 필요
		if annotations != nil {
			if capGB, ok := annotations["ai-storage.keti/available-capacity-gb"]; ok {
				if cap, err := strconv.ParseInt(capGB, 10, 64); err == nil {
					// 증가 비율에 따른 점수 조정
					requiredMultiplier := int64(chars.DataExpansion) + 1
					if cap >= 500*requiredMultiplier {
						score = maxScore
					} else if cap >= 200*requiredMultiplier {
						score = maxScore * 3 / 4
					} else if cap >= 100*requiredMultiplier {
						score = maxScore / 2
					} else {
						score = maxScore / 4
					}
				}
			}
		}
	} else if chars.DataExpansion < 1.0 {
		// 데이터 감소 - 스토리지 요구사항 낮음
		score = maxScore * 3 / 4 // 대부분의 노드가 적합
	}

	if score > maxScore {
		score = maxScore
	}

	return score
}

// calculateCSDScoreWithSource scores based on CSD offload capability with source tracking
func (p *IOPatternBased) calculateCSDScoreWithSource(policy *apollo.SchedulingPolicy, chars IOCharacteristics, node *v1.Node, policySource apollo.DataSource) (int64, string) {
	// Get max score from CRD config (defaults applied by configmanager)
	cfg := configmanager.GetManager().GetIOPatternConfig()
	maxScore := int64(cfg.Scoring.CSDScoreMax)

	// APOLLO에서 CSD 요구사항 확인
	requiresCSD := false
	source := "Node-Labels"

	if policy != nil && policySource != apollo.DataSourceFallback {
		requiresCSD = apollo.RequiresCSD(policy)
		if requiresCSD {
			source = "APOLLO-RequiresCSD"
		}
	}

	// Fallback to characteristics
	if !requiresCSD && chars.RequiresCSD {
		requiresCSD = true
		source = "IOCharacteristics"
	}

	if !requiresCSD {
		return maxScore / 2, "NEUTRAL (CSD not required)"
	}

	labels := node.Labels
	annotations := node.Annotations

	// CSD 노드 확인
	if labels != nil {
		if _, ok := labels["storage-tier/csd"]; ok {
			// CSD 성능 정보 확인
			if annotations != nil {
				if csdOps, ok := annotations["ai-storage.keti/csd-compute-ops"]; ok {
					if ops, err := strconv.ParseInt(csdOps, 10, 64); err == nil && ops > 0 {
						return maxScore, source + " + Node-CSD-HighPerf"
					}
				}
			}
			return maxScore * 9 / 10, source + " + Node-CSD"
		}
	}

	// 스토리지 레이어 노드 (CSD 없어도 가까움)
	if labels != nil {
		if layer, ok := labels["layer"]; ok && layer == "storage" {
			return maxScore * 6 / 10, source + " + Node-Storage-Layer"
		}
	}

	return maxScore / 4, source + " (no CSD)"
}

// calculateCSDScore scores based on CSD offload capability (legacy wrapper)
func (p *IOPatternBased) calculateCSDScore(policy *apollo.SchedulingPolicy, chars IOCharacteristics, node *v1.Node) int64 {
	score, _ := p.calculateCSDScoreWithSource(policy, chars, node, apollo.DataSourceAPOLLO)
	return score
}

// hasCSD checks if node has CSD capability
func (p *IOPatternBased) hasCSD(node *v1.Node) bool {
	labels := node.Labels
	if labels == nil {
		return false
	}

	if _, ok := labels["storage-tier/csd"]; ok {
		return true
	}
	if tier, ok := labels["storage-tier"]; ok {
		return strings.ToLower(tier) == "csd"
	}

	return false
}

// isCSDRequired checks if pod requires CSD
func (p *IOPatternBased) isCSDRequired(pod *v1.Pod) bool {
	if required, ok := pod.Annotations["ai-storage.keti/require-csd"]; ok {
		return required == "true"
	}
	return false
}
