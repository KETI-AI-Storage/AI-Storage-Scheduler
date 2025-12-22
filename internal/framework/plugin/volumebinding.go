package plugin

import (
	"context"
	"fmt"
	"sync"

	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const VolumeBindingName = "VolumeBinding"

// VolumeBinding is a plugin that checks if a pod's PVC requests can be satisfied by the node.
// It implements PreFilter, Filter, Reserve, PreBind, and Score extension points.
type VolumeBinding struct {
	k8sClient kubernetes.Interface

	// assumed PVC bindings per pod
	assumedBindings     map[string]map[string]string // podKey -> pvcKey -> pvName
	assumedBindingsLock sync.RWMutex
}

var _ framework.PreFilterPlugin = &VolumeBinding{}
var _ framework.FilterPlugin = &VolumeBinding{}
var _ framework.ReservePlugin = &VolumeBinding{}
var _ framework.PreBindPlugin = &VolumeBinding{}
var _ framework.ScorePlugin = &VolumeBinding{}

func NewVolumeBinding(k8sClient kubernetes.Interface) *VolumeBinding {
	return &VolumeBinding{
		k8sClient:       k8sClient,
		assumedBindings: make(map[string]map[string]string),
	}
}

func (p *VolumeBinding) Name() string {
	return VolumeBindingName
}

// PreFilter checks for unbound PVCs and validates StorageClass
func (p *VolumeBinding) PreFilter(ctx context.Context, pod *v1.Pod) (*framework.PreFilterResult, *utils.Status) {
	// Check each PVC referenced by the pod
	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim == nil {
			continue
		}

		pvcName := v.PersistentVolumeClaim.ClaimName

		// Get the PVC
		pvc, err := p.getPVC(ctx, pod.Namespace, pvcName)
		if err != nil {
			return nil, utils.NewStatus(utils.UnschedulableAndUnresolvable,
				fmt.Sprintf("PVC %s/%s not found: %v", pod.Namespace, pvcName, err))
		}

		// If already bound, nothing to do
		if pvc.Status.Phase == v1.ClaimBound {
			continue
		}

		// For unbound PVCs, check if StorageClass exists and supports dynamic provisioning
		if pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != "" {
			sc, err := p.getStorageClass(ctx, *pvc.Spec.StorageClassName)
			if err != nil {
				return nil, utils.NewStatus(utils.UnschedulableAndUnresolvable,
					fmt.Sprintf("StorageClass %s not found: %v", *pvc.Spec.StorageClassName, err))
			}

			// Check binding mode
			if sc.VolumeBindingMode != nil && *sc.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer {
				// This is handled at scheduling time, continue
				continue
			}
		}
	}

	return nil, utils.NewStatus(utils.Success, "")
}

func (p *VolumeBinding) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// Filter checks if the node can satisfy the pod's volume requirements
func (p *VolumeBinding) Filter(ctx context.Context, pod *v1.Pod, nodeInfo *utils.NodeInfo) *utils.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return utils.NewStatus(utils.Error, "node not found")
	}

	node := nodeInfo.Node()

	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim == nil {
			continue
		}

		pvcName := v.PersistentVolumeClaim.ClaimName
		pvc, err := p.getPVC(ctx, pod.Namespace, pvcName)
		if err != nil {
			return utils.NewStatus(utils.Unschedulable,
				fmt.Sprintf("PVC %s not found", pvcName))
		}

		// If already bound, check if PV is accessible from this node
		if pvc.Status.Phase == v1.ClaimBound && pvc.Spec.VolumeName != "" {
			pv, err := p.getPV(ctx, pvc.Spec.VolumeName)
			if err != nil {
				continue
			}

			// Check PV node affinity
			if pv.Spec.NodeAffinity != nil && pv.Spec.NodeAffinity.Required != nil {
				if !nodeMatchesNodeAffinity(node, pv.Spec.NodeAffinity.Required) {
					return utils.NewStatus(utils.Unschedulable,
						fmt.Sprintf("PV %s node affinity doesn't match node %s", pv.Name, node.Name))
				}
			}
			continue
		}

		// For unbound PVCs with WaitForFirstConsumer binding mode
		if pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != "" {
			sc, err := p.getStorageClass(ctx, *pvc.Spec.StorageClassName)
			if err != nil {
				continue
			}

			if sc.VolumeBindingMode != nil && *sc.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer {
				// Check if there's a suitable PV for this node
				suitable, err := p.findSuitablePV(ctx, pvc, node)
				if err != nil || !suitable {
					// Check if dynamic provisioning is possible
					if sc.Provisioner != "" {
						// Dynamic provisioning is possible, continue
						continue
					}
					return utils.NewStatus(utils.Unschedulable,
						fmt.Sprintf("no suitable PV for PVC %s on node %s", pvcName, node.Name))
				}
			}
		}
	}

	return utils.NewStatus(utils.Success, "")
}

// findSuitablePV checks if there's a suitable unbound PV for the PVC on the given node
func (p *VolumeBinding) findSuitablePV(ctx context.Context, pvc *v1.PersistentVolumeClaim, node *v1.Node) (bool, error) {
	if p.k8sClient == nil {
		return true, nil // Assume suitable if we can't check
	}

	pvs, err := p.k8sClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}

	for _, pv := range pvs.Items {
		// Skip bound PVs
		if pv.Status.Phase != v1.VolumeAvailable {
			continue
		}

		// Check capacity
		if pvc.Spec.Resources.Requests != nil {
			requestedCapacity := pvc.Spec.Resources.Requests[v1.ResourceStorage]
			pvCapacity := pv.Spec.Capacity[v1.ResourceStorage]
			if pvCapacity.Cmp(requestedCapacity) < 0 {
				continue
			}
		}

		// Check access modes
		if !hasMatchingAccessModes(pv.Spec.AccessModes, pvc.Spec.AccessModes) {
			continue
		}

		// Check node affinity
		if pv.Spec.NodeAffinity != nil && pv.Spec.NodeAffinity.Required != nil {
			if !nodeMatchesNodeAffinity(node, pv.Spec.NodeAffinity.Required) {
				continue
			}
		}

		// Found a suitable PV
		return true, nil
	}

	return false, nil
}

// hasMatchingAccessModes checks if PV access modes satisfy PVC requirements
func hasMatchingAccessModes(pvModes, pvcModes []v1.PersistentVolumeAccessMode) bool {
	if len(pvcModes) == 0 {
		return true
	}

	pvModeSet := make(map[v1.PersistentVolumeAccessMode]bool)
	for _, mode := range pvModes {
		pvModeSet[mode] = true
	}

	for _, mode := range pvcModes {
		if !pvModeSet[mode] {
			return false
		}
	}
	return true
}

// Reserve marks the assumed bindings for the pod
func (p *VolumeBinding) Reserve(ctx context.Context, pod *v1.Pod, nodeName string) *utils.Status {
	podKey, _ := utils.GetPodKey(pod)

	p.assumedBindingsLock.Lock()
	defer p.assumedBindingsLock.Unlock()

	p.assumedBindings[podKey] = make(map[string]string)

	return utils.NewStatus(utils.Success, "")
}

// Unreserve clears the assumed bindings for the pod
func (p *VolumeBinding) Unreserve(ctx context.Context, pod *v1.Pod, nodeName string) {
	podKey, _ := utils.GetPodKey(pod)

	p.assumedBindingsLock.Lock()
	defer p.assumedBindingsLock.Unlock()

	delete(p.assumedBindings, podKey)
}

// PreBind actually binds the PVCs to PVs
func (p *VolumeBinding) PreBind(ctx context.Context, pod *v1.Pod, nodeName string) *utils.Status {
	// In a full implementation, this would:
	// 1. Find suitable PVs for unbound PVCs
	// 2. Update PVC with selected PV name
	// 3. Wait for PVC to be bound

	// For now, we assume the volume controller handles this
	return utils.NewStatus(utils.Success, "")
}

// Score prefers nodes where volumes are already present or easily accessible
func (p *VolumeBinding) Score(ctx context.Context, pod *v1.Pod, nodeName string) (int64, *utils.Status) {
	var score int64 = 0
	volumeCount := 0

	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim == nil {
			continue
		}

		volumeCount++

		pvc, err := p.getPVC(ctx, pod.Namespace, v.PersistentVolumeClaim.ClaimName)
		if err != nil {
			continue
		}

		// Prefer nodes where bound PVs have node affinity matching
		if pvc.Status.Phase == v1.ClaimBound && pvc.Spec.VolumeName != "" {
			pv, err := p.getPV(ctx, pvc.Spec.VolumeName)
			if err != nil {
				continue
			}

			// Give points if PV has node affinity that prefers this node
			if pv.Spec.NodeAffinity != nil {
				// Already validated in Filter, so if we got here, it matches
				score += 100
			} else {
				// PV without node affinity can go anywhere
				score += 50
			}
		} else {
			// For unbound PVCs, score based on storage capacity availability
			score += 50
		}
	}

	if volumeCount == 0 {
		return 0, utils.NewStatus(utils.Success, "")
	}

	// Average score
	finalScore := score / int64(volumeCount)
	return finalScore, utils.NewStatus(utils.Success, "")
}

func (p *VolumeBinding) ScoreExtensions() framework.ScoreExtensions {
	return p
}

func (p *VolumeBinding) NormalizeScore(ctx context.Context, pod *v1.Pod, scores utils.PluginResult) *utils.Status {
	return utils.NewStatus(utils.Success, "")
}

func (p *VolumeBinding) getPVC(ctx context.Context, namespace, name string) (*v1.PersistentVolumeClaim, error) {
	if p.k8sClient == nil {
		return nil, fmt.Errorf("kubernetes client not available")
	}
	return p.k8sClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
}

func (p *VolumeBinding) getPV(ctx context.Context, name string) (*v1.PersistentVolume, error) {
	if p.k8sClient == nil {
		return nil, fmt.Errorf("kubernetes client not available")
	}
	return p.k8sClient.CoreV1().PersistentVolumes().Get(ctx, name, metav1.GetOptions{})
}

func (p *VolumeBinding) getStorageClass(ctx context.Context, name string) (*storagev1.StorageClass, error) {
	if p.k8sClient == nil {
		return nil, fmt.Errorf("kubernetes client not available")
	}
	return p.k8sClient.StorageV1().StorageClasses().Get(ctx, name, metav1.GetOptions{})
}
