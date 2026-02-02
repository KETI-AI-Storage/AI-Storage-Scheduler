// ============================================
// CSIStorageAware Plugin
// CSI (Container Storage Interface) 리소스 기반 스케줄링
// ============================================
//
// 이 플러그인은 Kubernetes CSI 리소스를 활용하여
// Pod의 PVC 요청에 최적화된 노드를 선택합니다.
//
// CSI 리소스 활용:
// 1. CSINode: 노드에 설치된 CSI 드라이버 확인
// 2. CSIStorageCapacity: 노드별 CSI 스토리지 가용 용량 확인
// 3. StorageClass: PVC의 CSI provisioner 확인
//
// Filter (조건부 - Pod에 PVC가 있을 때만):
// - PVC의 StorageClass가 CSI provisioner 사용 시:
//   - 해당 노드에 CSI 드라이버 설치 여부 확인 (CSINode)
//   - CSIStorageCapacity를 통한 가용 용량 확인
//
// Score (조건부 - Pod에 PVC가 있을 때만):
// - CSI 스토리지 가용 용량 비율 (0-40점)
// - CSI 드라이버 볼륨 수 여유분 (0-30점)
// - CSI 토폴로지 매칭 (0-30점)
//
// PVC가 없는 Pod는 Filter 통과, Score 중립(50)
// ============================================

package plugin

import (
	"context"
	"fmt"
	"strings"

	logger "keti/ai-storage-scheduler/internal/backend/log"
	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

const CSIStorageAwareName = "CSIStorageAware"

// CSIStorageAware scores and filters nodes based on CSI storage resources.
// It checks CSINode for driver availability, CSIStorageCapacity for
// available capacity, and StorageClass for CSI provisioner information.
type CSIStorageAware struct {
	cache      *utils.Cache
	kubeClient kubernetes.Interface
}

var _ framework.FilterPlugin = &CSIStorageAware{}
var _ framework.ScorePlugin = &CSIStorageAware{}

// NewCSIStorageAware creates a new CSIStorageAware plugin
func NewCSIStorageAware(cache *utils.Cache, kubeClient kubernetes.Interface) *CSIStorageAware {
	return &CSIStorageAware{
		cache:      cache,
		kubeClient: kubeClient,
	}
}

func (c *CSIStorageAware) Name() string {
	return CSIStorageAwareName
}

// ============================================
// Filter Phase
// ============================================

// Filter checks if the node can satisfy the pod's CSI storage requirements.
// If the pod has no PVCs, the filter passes immediately.
func (c *CSIStorageAware) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	node := nodeInfo.Node()
	if node == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	// Pod에 PVC가 없으면 무조건 통과
	pvcInfos := c.getPodPVCInfos(ctx, pod)
	if len(pvcInfos) == 0 {
		return utils.NewStatus(utils.Success, "")
	}

	// CSINode 정보 가져오기
	csiNode := c.getCSINode(ctx, node.Name)

	for _, pvcInfo := range pvcInfos {
		// CSI provisioner가 아닌 경우 스킵
		if pvcInfo.provisioner == "" || !c.isCSIProvisioner(pvcInfo.provisioner) {
			continue
		}

		// 1. CSI 드라이버가 노드에 설치되어 있는지 확인
		if csiNode != nil {
			if !c.nodeHasCSIDriver(csiNode, pvcInfo.provisioner) {
				return utils.NewStatus(utils.Unschedulable,
					fmt.Sprintf("CSI driver %s not installed on node %s",
						pvcInfo.provisioner, node.Name))
			}
		}

		// 2. CSIStorageCapacity로 용량 확인 (가용 데이터가 있는 경우)
		if pvcInfo.requestedBytes > 0 {
			hasCapacity, err := c.nodeHasCSICapacity(ctx, node, pvcInfo.storageClassName, pvcInfo.requestedBytes)
			if err != nil {
				// CSIStorageCapacity 조회 실패 시 허용 (정보 부족으로 필터링하지 않음)
				logger.Info("[CSIStorageAware] CSIStorageCapacity check failed, allowing node",
					"node", node.Name, "error", err.Error())
				continue
			}
			if !hasCapacity {
				return utils.NewStatus(utils.Unschedulable,
					fmt.Sprintf("insufficient CSI storage capacity on node %s for StorageClass %s (requested: %d bytes)",
						node.Name, pvcInfo.storageClassName, pvcInfo.requestedBytes))
			}
		}
	}

	return utils.NewStatus(utils.Success, "")
}

// ============================================
// Score Phase
// ============================================

// Score scores nodes based on CSI storage availability.
// If the pod has no PVCs, returns neutral score (50).
func (c *CSIStorageAware) Score(ctx context.Context, pod *v1.Pod, nodeName string) (int64, *utils.Status) {
	nodeInfo := c.cache.Nodes()[nodeName]
	if nodeInfo == nil {
		return 0, utils.NewStatus(utils.Error, "node not found in cache")
	}
	node := nodeInfo.Node()
	if node == nil {
		return 0, utils.NewStatus(utils.Error, "node not found")
	}

	// Pod에 PVC가 없으면 중립 점수
	pvcInfos := c.getPodPVCInfos(ctx, pod)
	if len(pvcInfos) == 0 {
		return 50, utils.NewStatus(utils.Success, "")
	}

	// CSI PVC만 필터링
	csiPVCInfos := make([]pvcInfo, 0)
	for _, info := range pvcInfos {
		if info.provisioner != "" && c.isCSIProvisioner(info.provisioner) {
			csiPVCInfos = append(csiPVCInfos, info)
		}
	}

	// CSI PVC가 없으면 중립 점수
	if len(csiPVCInfos) == 0 {
		return 50, utils.NewStatus(utils.Success, "")
	}

	// 1. CSI 스토리지 가용 용량 점수 (0-40점)
	capacityScore := c.calculateCapacityScore(ctx, node, csiPVCInfos)

	// 2. CSI 드라이버 볼륨 여유분 점수 (0-30점)
	volumeHeadroomScore := c.calculateVolumeHeadroomScore(ctx, node, nodeInfo, csiPVCInfos)

	// 3. CSI 토폴로지 매칭 점수 (0-30점)
	topologyScore := c.calculateTopologyScore(ctx, node, csiPVCInfos)

	totalScore := capacityScore + volumeHeadroomScore + topologyScore

	logger.Info("[CSIStorageAware] Node scored",
		"node", nodeName,
		"totalScore", totalScore,
		"capacityScore", capacityScore,
		"volumeHeadroomScore", volumeHeadroomScore,
		"topologyScore", topologyScore,
		"csiPVCCount", len(csiPVCInfos))

	return totalScore, utils.NewStatus(utils.Success, "")
}

func (c *CSIStorageAware) ScoreExtensions() framework.ScoreExtensions {
	return c
}

func (c *CSIStorageAware) NormalizeScore(ctx context.Context, pod *v1.Pod, scores utils.PluginResult) *utils.Status {
	return utils.NewStatus(utils.Success, "")
}

// ============================================
// Internal Types
// ============================================

// pvcInfo holds information about a PVC relevant for CSI scheduling
type pvcInfo struct {
	pvcName          string
	pvcNamespace     string
	storageClassName string
	provisioner      string
	requestedBytes   int64
	isBound          bool
	accessModes      []v1.PersistentVolumeAccessMode
}

// ============================================
// PVC Information Extraction
// ============================================

// getPodPVCInfos extracts PVC information from a pod's volumes
func (c *CSIStorageAware) getPodPVCInfos(ctx context.Context, pod *v1.Pod) []pvcInfo {
	if c.kubeClient == nil {
		return nil
	}

	var infos []pvcInfo

	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		pvcName := volume.PersistentVolumeClaim.ClaimName
		info := pvcInfo{
			pvcName:      pvcName,
			pvcNamespace: pod.Namespace,
		}

		// PVC 가져오기
		pvc, err := c.kubeClient.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			logger.Info("[CSIStorageAware] Failed to get PVC, skipping",
				"pvc", pvcName, "error", err.Error())
			continue
		}

		info.isBound = pvc.Status.Phase == v1.ClaimBound
		info.accessModes = pvc.Spec.AccessModes

		// 요청 용량 추출
		if storageReq, ok := pvc.Spec.Resources.Requests[v1.ResourceStorage]; ok {
			info.requestedBytes = storageReq.Value()
		}

		// StorageClass 확인
		if pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != "" {
			info.storageClassName = *pvc.Spec.StorageClassName

			// StorageClass에서 provisioner 가져오기
			sc, err := c.kubeClient.StorageV1().StorageClasses().Get(ctx, info.storageClassName, metav1.GetOptions{})
			if err != nil {
				logger.Info("[CSIStorageAware] Failed to get StorageClass",
					"storageClass", info.storageClassName, "error", err.Error())
			} else {
				info.provisioner = sc.Provisioner
			}
		}

		infos = append(infos, info)
	}

	return infos
}

// ============================================
// CSI Resource Access
// ============================================

// getCSINode retrieves the CSINode resource for a given node
func (c *CSIStorageAware) getCSINode(ctx context.Context, nodeName string) *storagev1.CSINode {
	if c.kubeClient == nil {
		return nil
	}

	csiNode, err := c.kubeClient.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		// CSINode가 없는 노드는 CSI 드라이버 미설치
		return nil
	}
	return csiNode
}

// nodeHasCSIDriver checks if a CSI driver is installed on the node
func (c *CSIStorageAware) nodeHasCSIDriver(csiNode *storagev1.CSINode, driverName string) bool {
	if csiNode == nil {
		return false
	}

	for _, driver := range csiNode.Spec.Drivers {
		if driver.Name == driverName {
			return true
		}
	}
	return false
}

// getCSIDriverInfo returns the CSINodeDriver info for a specific driver on a node
func (c *CSIStorageAware) getCSIDriverInfo(csiNode *storagev1.CSINode, driverName string) *storagev1.CSINodeDriver {
	if csiNode == nil {
		return nil
	}

	for i := range csiNode.Spec.Drivers {
		if csiNode.Spec.Drivers[i].Name == driverName {
			return &csiNode.Spec.Drivers[i]
		}
	}
	return nil
}

// nodeHasCSICapacity checks if a node has sufficient CSI storage capacity
// using CSIStorageCapacity resources
func (c *CSIStorageAware) nodeHasCSICapacity(ctx context.Context, node *v1.Node, storageClassName string, requestedBytes int64) (bool, error) {
	if c.kubeClient == nil {
		return true, nil
	}

	if storageClassName == "" {
		return true, nil
	}

	// CSIStorageCapacity 리소스 조회 (모든 네임스페이스)
	capacities, err := c.kubeClient.StorageV1().CSIStorageCapacities("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("failed to list CSIStorageCapacities: %w", err)
	}

	if len(capacities.Items) == 0 {
		// CSIStorageCapacity가 없으면 용량 확인 불가 → 허용
		return true, nil
	}

	// 해당 StorageClass의 CSIStorageCapacity 중 이 노드에 매칭되는 것 찾기
	for _, cap := range capacities.Items {
		if cap.StorageClassName != storageClassName {
			continue
		}

		// 노드 토폴로지 매칭 확인
		if !c.nodeMatchesTopology(node, cap.NodeTopology) {
			continue
		}

		// MaximumVolumeSize 확인
		if cap.MaximumVolumeSize != nil {
			maxSize := cap.MaximumVolumeSize.Value()
			if requestedBytes > maxSize {
				return false, nil
			}
		}

		// Capacity (가용 용량) 확인
		if cap.Capacity != nil {
			available := cap.Capacity.Value()
			if requestedBytes > available {
				return false, nil
			}
		}

		// 매칭되는 CSIStorageCapacity가 있고 용량이 충분함
		return true, nil
	}

	// 매칭되는 CSIStorageCapacity가 없으면 허용 (정보 부족)
	return true, nil
}

// nodeMatchesTopology checks if a node matches the topology selector
func (c *CSIStorageAware) nodeMatchesTopology(node *v1.Node, topology *metav1.LabelSelector) bool {
	if topology == nil {
		return true // nil topology = all nodes
	}

	nodeLabels := node.Labels
	if nodeLabels == nil {
		nodeLabels = make(map[string]string)
	}

	selector, err := metav1.LabelSelectorAsSelector(topology)
	if err != nil {
		return false
	}

	return selector.Matches(labels.Set(nodeLabels))
}

// isCSIProvisioner checks if a provisioner is a CSI driver
// (non-CSI provisioners are kubernetes.io/* built-in provisioners)
func (c *CSIStorageAware) isCSIProvisioner(provisioner string) bool {
	// Built-in (non-CSI) provisioners start with "kubernetes.io/"
	if strings.HasPrefix(provisioner, "kubernetes.io/") {
		return false
	}
	// Everything else is a CSI provisioner
	return true
}

// ============================================
// Score Calculation Functions
// ============================================

// calculateCapacityScore scores based on available CSI storage capacity (0-40)
func (c *CSIStorageAware) calculateCapacityScore(ctx context.Context, node *v1.Node, pvcInfos []pvcInfo) int64 {
	const maxScore int64 = 40

	if c.kubeClient == nil {
		return maxScore / 2
	}

	// CSIStorageCapacity 리소스 조회
	capacities, err := c.kubeClient.StorageV1().CSIStorageCapacities("").List(ctx, metav1.ListOptions{})
	if err != nil || len(capacities.Items) == 0 {
		return maxScore / 2 // 정보 없으면 중립
	}

	totalScore := int64(0)
	scoredCount := 0

	for _, pvcInfo := range pvcInfos {
		if pvcInfo.storageClassName == "" {
			continue
		}

		// 이 StorageClass에 대한 노드의 가용 용량 찾기
		nodeCapacity := c.getNodeCapacityForStorageClass(node, capacities, pvcInfo.storageClassName)

		if nodeCapacity == nil {
			totalScore += maxScore / 2 // 정보 없으면 중립
			scoredCount++
			continue
		}

		availableBytes := nodeCapacity.Value()
		requestedBytes := pvcInfo.requestedBytes

		if requestedBytes <= 0 {
			// 요청량 미지정 → 가용 용량 크기 기반 점수
			if availableBytes >= 1*1024*1024*1024*1024 { // 1TB+
				totalScore += maxScore
			} else if availableBytes >= 500*1024*1024*1024 { // 500GB+
				totalScore += maxScore * 8 / 10
			} else if availableBytes >= 100*1024*1024*1024 { // 100GB+
				totalScore += maxScore * 6 / 10
			} else if availableBytes >= 10*1024*1024*1024 { // 10GB+
				totalScore += maxScore * 4 / 10
			} else {
				totalScore += maxScore * 2 / 10
			}
		} else {
			// 요청량 대비 가용 용량 비율 기반 점수
			ratio := float64(availableBytes) / float64(requestedBytes)
			if ratio >= 10.0 {
				totalScore += maxScore // 충분한 여유
			} else if ratio >= 5.0 {
				totalScore += maxScore * 9 / 10
			} else if ratio >= 2.0 {
				totalScore += maxScore * 7 / 10
			} else if ratio >= 1.5 {
				totalScore += maxScore * 5 / 10
			} else if ratio >= 1.0 {
				totalScore += maxScore * 3 / 10
			} else {
				totalScore += 0
			}
		}
		scoredCount++
	}

	if scoredCount == 0 {
		return maxScore / 2
	}

	return totalScore / int64(scoredCount)
}

// getNodeCapacityForStorageClass finds the available capacity for a node and storage class
func (c *CSIStorageAware) getNodeCapacityForStorageClass(node *v1.Node, capacities *storagev1.CSIStorageCapacityList, storageClassName string) *resource.Quantity {
	for _, cap := range capacities.Items {
		if cap.StorageClassName != storageClassName {
			continue
		}

		if !c.nodeMatchesTopology(node, cap.NodeTopology) {
			continue
		}

		if cap.Capacity != nil {
			return cap.Capacity
		}
	}
	return nil
}

// calculateVolumeHeadroomScore scores based on CSI volume count headroom (0-30)
func (c *CSIStorageAware) calculateVolumeHeadroomScore(ctx context.Context, node *v1.Node, nodeInfo *utils.NodeInfo, pvcInfos []pvcInfo) int64 {
	const maxScore int64 = 30

	csiNode := c.getCSINode(ctx, node.Name)
	if csiNode == nil {
		return maxScore / 2 // CSINode 없으면 중립
	}

	// 드라이버별 사용 중인 볼륨 수 계산
	existingVolumes := countCSIVolumesByDriver(nodeInfo)

	totalScore := int64(0)
	scoredCount := 0

	// 고유한 드라이버 목록
	driversSeen := make(map[string]bool)

	for _, pvcInfo := range pvcInfos {
		if driversSeen[pvcInfo.provisioner] {
			continue
		}
		driversSeen[pvcInfo.provisioner] = true

		driverInfo := c.getCSIDriverInfo(csiNode, pvcInfo.provisioner)
		if driverInfo == nil {
			totalScore += maxScore / 2
			scoredCount++
			continue
		}

		// 드라이버의 볼륨 제한 확인
		maxVolumes := int64(DefaultMaxCSIVolumes)
		if driverInfo.Allocatable != nil && driverInfo.Allocatable.Count != nil {
			maxVolumes = int64(*driverInfo.Allocatable.Count)
		}

		existingCount := int64(existingVolumes[pvcInfo.provisioner])
		headroom := maxVolumes - existingCount

		// 여유 비율 기반 점수
		if maxVolumes > 0 {
			headroomRatio := float64(headroom) / float64(maxVolumes)
			if headroomRatio >= 0.8 {
				totalScore += maxScore // 80%+ 여유
			} else if headroomRatio >= 0.5 {
				totalScore += maxScore * 8 / 10 // 50%+ 여유
			} else if headroomRatio >= 0.3 {
				totalScore += maxScore * 5 / 10 // 30%+ 여유
			} else if headroomRatio >= 0.1 {
				totalScore += maxScore * 3 / 10 // 10%+ 여유
			} else {
				totalScore += maxScore / 10 // 거의 꽉 참
			}
		} else {
			totalScore += maxScore / 2
		}

		scoredCount++
	}

	if scoredCount == 0 {
		return maxScore / 2
	}

	return totalScore / int64(scoredCount)
}

// calculateTopologyScore scores based on CSI topology matching (0-30)
func (c *CSIStorageAware) calculateTopologyScore(ctx context.Context, node *v1.Node, pvcInfos []pvcInfo) int64 {
	const maxScore int64 = 30

	if c.kubeClient == nil {
		return maxScore / 2
	}

	csiNode := c.getCSINode(ctx, node.Name)
	if csiNode == nil {
		return maxScore / 2
	}

	totalScore := int64(0)
	scoredCount := 0

	driversSeen := make(map[string]bool)

	for _, pvcInfo := range pvcInfos {
		if driversSeen[pvcInfo.provisioner] {
			continue
		}
		driversSeen[pvcInfo.provisioner] = true

		driverInfo := c.getCSIDriverInfo(csiNode, pvcInfo.provisioner)
		if driverInfo == nil {
			totalScore += maxScore / 3
			scoredCount++
			continue
		}

		// 드라이버의 토폴로지 키 확인
		if len(driverInfo.TopologyKeys) > 0 {
			// 노드에 토폴로지 키가 매칭되는지 확인
			matchedKeys := 0
			for _, key := range driverInfo.TopologyKeys {
				if _, exists := node.Labels[key]; exists {
					matchedKeys++
				}
			}

			if matchedKeys == len(driverInfo.TopologyKeys) {
				totalScore += maxScore // 모든 토폴로지 키 매칭
			} else if matchedKeys > 0 {
				// 부분 매칭
				ratio := float64(matchedKeys) / float64(len(driverInfo.TopologyKeys))
				totalScore += int64(float64(maxScore) * ratio)
			} else {
				totalScore += maxScore / 4 // 토폴로지 키 미매칭
			}
		} else {
			// 토폴로지 키가 없으면 중립
			totalScore += maxScore * 2 / 3
		}

		scoredCount++
	}

	if scoredCount == 0 {
		return maxScore / 2
	}

	return totalScore / int64(scoredCount)
}
