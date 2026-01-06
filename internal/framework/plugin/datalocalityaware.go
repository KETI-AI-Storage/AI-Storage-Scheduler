// ============================================
// DataLocalityAware Plugin
// 데이터 노드 우선 배치, 네트워크 전송 최소화
// ============================================
//
// 이 플러그인은 APOLLO로부터 워크로드 분석 결과를 받아
// 전처리 워크로드의 입력 데이터가 위치한 노드에
// 우선적으로 스케줄링하여 네트워크 전송을 최소화합니다.
//
// APOLLO 연동:
// - insight-trace가 분석한 데이터 로컬리티 정보 활용
// - 노드별 데이터 캐싱 상태 확인
// - APOLLO의 NodePreferences 점수 반영
//
// 점수 산정 기준:
// 1. APOLLO NodePreference 점수 (0-30점) - APOLLO 분석 결과
// 2. PVC가 바인딩된 노드 (로컬 스토리지) - 최우선 (0-30점)
// 3. 데이터셋이 캐싱된 노드 - 우선 (0-20점)
// 4. 네트워크 토폴로지 상 가까운 노드 - 일반 (0-20점)
// ============================================

package plugin

import (
	"context"
	"strings"

	"keti/ai-storage-scheduler/internal/apollo"
	logger "keti/ai-storage-scheduler/internal/backend/log"
	"keti/ai-storage-scheduler/internal/configmanager"
	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const DataLocalityAwareName = "DataLocalityAware"

// DataLocalityAware scores nodes based on data locality for preprocessing workloads
type DataLocalityAware struct {
	cache        *utils.Cache
	kubeClient   kubernetes.Interface
	apolloClient *apollo.Client
}

var _ framework.ScorePlugin = &DataLocalityAware{}
var _ framework.FilterPlugin = &DataLocalityAware{}

// NewDataLocalityAware creates a new DataLocalityAware plugin
func NewDataLocalityAware(cache *utils.Cache, kubeClient kubernetes.Interface) *DataLocalityAware {
	return &DataLocalityAware{
		cache:        cache,
		kubeClient:   kubeClient,
		apolloClient: apollo.GetClient(),
	}
}

func (d *DataLocalityAware) Name() string {
	return DataLocalityAwareName
}

// Filter filters out nodes that cannot satisfy data locality requirements
func (d *DataLocalityAware) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	node := nodeInfo.Node()
	if node == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	// Get scheduling policy from APOLLO
	policy := d.getSchedulingPolicy(pod)

	// 전처리 워크로드가 아니면 필터링 안함
	if !apollo.IsPreprocessingWorkload(policy) && !d.isPreprocessingWorkloadFromPod(pod) {
		return utils.NewStatus(utils.Success, "")
	}

	// ReadWriteOnce PVC가 있고 이미 다른 노드에 바인딩된 경우 필터링
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			pvcName := volume.PersistentVolumeClaim.ClaimName
			if !d.canAccessPVC(pod.Namespace, pvcName, node.Name) {
				return utils.NewStatus(utils.Unschedulable,
					"PVC %s is not accessible from node %s", pvcName, node.Name)
			}
		}
	}

	return utils.NewStatus(utils.Success, "")
}

// Score scores nodes based on data locality
func (d *DataLocalityAware) Score(ctx context.Context, pod *v1.Pod, nodeName string) (int64, *utils.Status) {
	nodeInfo := d.cache.Nodes()[nodeName]
	if nodeInfo == nil {
		return 0, utils.NewStatus(utils.Error, "node not found in cache")
	}

	node := nodeInfo.Node()
	if node == nil {
		return 0, utils.NewStatus(utils.Error, "node not found")
	}

	// Get scheduling policy from APOLLO
	policy := d.getSchedulingPolicy(pod)

	// 전처리 워크로드가 아니면 기본 점수
	if !apollo.IsPreprocessingWorkload(policy) && !d.isPreprocessingWorkloadFromPod(pod) {
		return 50, utils.NewStatus(utils.Success, "")
	}

	score := int64(0)

	apolloScore := d.calculateAPOLLOScore(policy, nodeName)
	score += apolloScore

	pvcScore := d.calculatePVCLocalityScore(pod, node)
	score += pvcScore

	cacheScore := d.calculateDataCacheScore(policy, node)
	score += cacheScore

	topologyScore := d.calculateTopologyScore(pod, node)
	score += topologyScore

	logger.Info("[DataLocalityAware] Node scored",
		"node", nodeName, "score", score,
		"apolloScore", apolloScore, "pvcScore", pvcScore,
		"cacheScore", cacheScore, "topologyScore", topologyScore)

	return score, utils.NewStatus(utils.Success, "")
}

func (d *DataLocalityAware) ScoreExtensions() framework.ScoreExtensions {
	return d
}

func (d *DataLocalityAware) NormalizeScore(ctx context.Context, pod *v1.Pod, scores utils.PluginResult) *utils.Status {
	return utils.NewStatus(utils.Success, "")
}

// getSchedulingPolicy fetches scheduling policy from APOLLO
func (d *DataLocalityAware) getSchedulingPolicy(pod *v1.Pod) *apollo.SchedulingPolicy {
	if d.apolloClient == nil {
		return nil
	}

	policy, err := d.apolloClient.GetSchedulingPolicy(
		pod.Namespace,
		pod.Name,
		string(pod.UID),
		pod.Labels,
		pod.Annotations,
	)
	if err != nil {
		logger.Warn("[DataLocalityAware] Failed to get APOLLO policy", "error", err.Error())
		return nil
	}

	return policy
}

// calculateAPOLLOScore calculates score based on APOLLO's node preferences
func (d *DataLocalityAware) calculateAPOLLOScore(policy *apollo.SchedulingPolicy, nodeName string) int64 {
	// Get max score from CRD config
	cfg := configmanager.GetManager().GetDataLocalityConfig()
	maxScore := int64(cfg.Scoring.ApolloScoreMax)
	if maxScore == 0 {
		maxScore = 30 // default
	}

	if policy == nil {
		return maxScore / 2 // 중립 점수
	}

	// APOLLO NodePreference에서 이 노드의 점수 확인
	prefScore := apollo.GetNodePreferenceScore(policy, nodeName)
	if prefScore > 0 {
		// APOLLO 점수를 0-maxScore 범위로 매핑 (APOLLO는 0-100)
		return int64(prefScore) * maxScore / 100
	}

	// 데이터 위치 노드인지 확인
	dataLocations := apollo.GetDataLocations(policy)
	for _, loc := range dataLocations {
		if loc == nodeName {
			return maxScore // 데이터가 있는 노드는 최고 점수
		}
	}

	return maxScore / 3 // 기본 점수
}

// isPreprocessingWorkloadFromPod checks pod labels/annotations directly (fallback)
func (d *DataLocalityAware) isPreprocessingWorkloadFromPod(pod *v1.Pod) bool {
	// Check labels
	if stage, ok := pod.Labels["pipeline-step"]; ok {
		if stage == "preprocess" || stage == "preprocessing" {
			return true
		}
	}

	if stage, ok := pod.Labels["stage"]; ok {
		if stage == "preprocess" || stage == "preprocessing" || stage == "data-loading" {
			return true
		}
	}

	// Check workload type annotation
	if wtype, ok := pod.Annotations["ai-storage.keti/workload-stage"]; ok {
		if strings.Contains(strings.ToLower(wtype), "preprocess") {
			return true
		}
	}

	return false
}

// calculatePVCLocalityScore calculates score based on PVC locality
func (d *DataLocalityAware) calculatePVCLocalityScore(pod *v1.Pod, node *v1.Node) int64 {
	// Get max score from CRD config
	cfg := configmanager.GetManager().GetDataLocalityConfig()
	maxScore := int64(cfg.Scoring.PVCLocalityScoreMax)
	if maxScore == 0 {
		maxScore = 30 // default
	}

	if len(pod.Spec.Volumes) == 0 {
		return maxScore / 2 // No volumes, neutral score
	}

	totalScore := int64(0)
	pvcCount := 0

	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		pvcCount++

		pvcName := volume.PersistentVolumeClaim.ClaimName

		// Check if PVC has node affinity to this node
		if d.isPVCLocalToNode(pod.Namespace, pvcName, node.Name) {
			totalScore += maxScore // Maximum locality score
		} else if d.isPVCAccessibleFromNode(pod.Namespace, pvcName, node.Name) {
			totalScore += maxScore / 2 // Accessible but not local
		}
	}

	if pvcCount == 0 {
		return maxScore / 2
	}

	return totalScore / int64(pvcCount)
}

// isPVCLocalToNode checks if PVC is local to the specified node
func (d *DataLocalityAware) isPVCLocalToNode(namespace, pvcName, nodeName string) bool {
	if d.kubeClient == nil {
		return false
	}

	ctx := context.Background()

	// Get PVC
	pvc, err := d.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		return false
	}

	// If PVC is not bound, no locality
	if pvc.Status.Phase != v1.ClaimBound || pvc.Spec.VolumeName == "" {
		return false
	}

	// Get PV
	pv, err := d.kubeClient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return false
	}

	// Check PV node affinity
	if pv.Spec.NodeAffinity != nil && pv.Spec.NodeAffinity.Required != nil {
		for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
			for _, expr := range term.MatchExpressions {
				if expr.Key == "kubernetes.io/hostname" {
					for _, value := range expr.Values {
						if value == nodeName {
							return true
						}
					}
				}
			}
		}
	}

	// Check local PV path (if it's a local PV)
	if pv.Spec.Local != nil {
		if pv.Spec.NodeAffinity != nil {
			return true // Already checked above
		}
	}

	return false
}

// isPVCAccessibleFromNode checks if PVC is accessible from the node
func (d *DataLocalityAware) isPVCAccessibleFromNode(namespace, pvcName, nodeName string) bool {
	if d.kubeClient == nil {
		return true // Assume accessible if can't check
	}

	ctx := context.Background()

	pvc, err := d.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		return true
	}

	// ReadWriteMany can be accessed from any node
	for _, accessMode := range pvc.Spec.AccessModes {
		if accessMode == v1.ReadWriteMany || accessMode == v1.ReadOnlyMany {
			return true
		}
	}

	// ReadWriteOnce - check if bound PV is accessible
	if pvc.Status.Phase == v1.ClaimBound {
		return d.isPVCLocalToNode(namespace, pvcName, nodeName)
	}

	return true // Unbound PVC can be bound anywhere
}

// canAccessPVC checks if a node can access the PVC
func (d *DataLocalityAware) canAccessPVC(namespace, pvcName, nodeName string) bool {
	if d.kubeClient == nil {
		return true
	}

	ctx := context.Background()

	pvc, err := d.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		return true
	}

	// ReadWriteMany/ReadOnlyMany can be accessed from any node
	for _, accessMode := range pvc.Spec.AccessModes {
		if accessMode == v1.ReadWriteMany || accessMode == v1.ReadOnlyMany {
			return true
		}
	}

	// ReadWriteOnce - if bound, check node affinity
	if pvc.Status.Phase == v1.ClaimBound {
		return d.isPVCLocalToNode(namespace, pvcName, nodeName)
	}

	return true // Unbound - can be bound to this node
}

// calculateDataCacheScore scores based on cached datasets on the node
// APOLLO에서 받은 캐시 노드 정보 사용
func (d *DataLocalityAware) calculateDataCacheScore(policy *apollo.SchedulingPolicy, node *v1.Node) int64 {
	// Get max score from CRD config
	cfg := configmanager.GetManager().GetDataLocalityConfig()
	maxScore := int64(cfg.Scoring.CacheScoreMax)
	if maxScore == 0 {
		maxScore = 20 // default
	}

	score := int64(0)

	// APOLLO에서 캐시된 노드 정보 확인
	if policy != nil {
		cachedNodes := apollo.GetCachedNodes(policy)
		for _, cachedNode := range cachedNodes {
			if cachedNode == node.Name {
				return maxScore // 캐시된 노드는 최고 점수
			}
		}
	}

	// 노드 어노테이션에서 캐시 정보 확인 (fallback)
	annotations := node.Annotations
	if annotations == nil {
		return maxScore / 4
	}

	// Check for cached dataset annotations (set by insight-scope)
	if _, ok := annotations["ai-storage.keti/cached-datasets"]; ok {
		score = maxScore * 3 / 4
	}

	// Check for warm data indicator
	if warmData, ok := annotations["ai-storage.keti/data-temperature"]; ok {
		if warmData == "hot" || warmData == "warm" {
			score += maxScore / 4
		}
	}

	if score > maxScore {
		score = maxScore
	}

	return score
}

// calculateTopologyScore scores based on network topology proximity
func (d *DataLocalityAware) calculateTopologyScore(pod *v1.Pod, node *v1.Node) int64 {
	// Get max score from CRD config
	cfg := configmanager.GetManager().GetDataLocalityConfig()
	maxScore := int64(cfg.Scoring.TopologyScoreMax)
	if maxScore == 0 {
		maxScore = 20 // default
	}

	labels := node.Labels
	if labels == nil {
		return maxScore / 2
	}

	score := maxScore / 2

	// Check topology zone - prefer same zone as data source
	if zone, ok := labels["topology.kubernetes.io/zone"]; ok {
		dataZone := d.getPreferredZone(pod)
		if dataZone != "" && zone == dataZone {
			score = maxScore
		}
	}

	// Check rack topology for data center aware scheduling
	if rack, ok := labels["topology.kubernetes.io/rack"]; ok {
		dataRack := d.getPreferredRack(pod)
		if dataRack != "" && rack == dataRack {
			score = maxScore
		}
	}

	return score
}

// getPreferredZone extracts preferred zone from pod spec or annotations
func (d *DataLocalityAware) getPreferredZone(pod *v1.Pod) string {
	if zone, ok := pod.Annotations["ai-storage.keti/preferred-zone"]; ok {
		return zone
	}

	// Check affinity for zone preference
	if pod.Spec.Affinity != nil && pod.Spec.Affinity.NodeAffinity != nil {
		pref := pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
		for _, term := range pref {
			for _, expr := range term.Preference.MatchExpressions {
				if expr.Key == "topology.kubernetes.io/zone" && len(expr.Values) > 0 {
					return expr.Values[0]
				}
			}
		}
	}

	return ""
}

// getPreferredRack extracts preferred rack from pod annotations
func (d *DataLocalityAware) getPreferredRack(pod *v1.Pod) string {
	if rack, ok := pod.Annotations["ai-storage.keti/preferred-rack"]; ok {
		return rack
	}
	return ""
}
