package queue

import (
	"slices"
	"sync"

	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
)

type nominator struct {
	// nLock synchronizes all operations related to nominator.
	// It should not be used anywhere else.
	// Caution: DO NOT take ("SchedulingQueue.lock" or "activeQueue.lock") after taking "nLock".
	// You should always take "SchedulingQueue.lock" and "activeQueue.lock" first,
	// otherwise the nominator could end up in deadlock.
	// Correct locking order is: SchedulingQueue.lock > activeQueue.lock > nLock.
	nLock sync.RWMutex

	// podLister is used to verify if the given pod is alive.
	podLister listersv1.PodLister
	// nominatedPods is a map keyed by a node name and the value is a list of
	// pods which are nominated to run on the node. These are pods which can be in
	// the activeQ or unschedulablePods.
	nominatedPods map[string][]podRef
	// nominatedPodToNode is map keyed by a Pod UID to the node name where it is
	// nominated.
	nominatedPodToNode map[types.UID]string
}

func newPodNominator(podLister listersv1.PodLister) *nominator {
	return &nominator{
		podLister:          podLister,
		nominatedPods:      make(map[string][]podRef),
		nominatedPodToNode: make(map[types.UID]string),
	}
}

// AddNominatedPod adds a pod to the nominated pods of the given node.
// This is called during the preemption process after a node is nominated to run
// the pod. We update the structure before sending a request to update the pod
// object to avoid races with the following scheduling cycles.
func (npm *nominator) AddNominatedPod(logger klog.Logger, pi *utils.PodInfo, nominatingInfo *utils.NominatingInfo) {
	npm.nLock.Lock()
	npm.addNominatedPodUnlocked(logger, pi, nominatingInfo)
	npm.nLock.Unlock()
}

func (npm *nominator) addNominatedPodUnlocked(logger klog.Logger, pi *utils.PodInfo, nominatingInfo *utils.NominatingInfo) {
	// Always delete the pod if it already exists, to ensure we never store more than
	// one instance of the pod.
	npm.deleteUnlocked(pi.Pod)

	var nodeName string
	if nominatingInfo.Mode() == utils.ModeOverride {
		nodeName = nominatingInfo.NominatedNodeName
	} else if nominatingInfo.Mode() == utils.ModeNoop {
		if pi.Pod.Status.NominatedNodeName == "" {
			return
		}
		nodeName = pi.Pod.Status.NominatedNodeName
	}

	if npm.podLister != nil {
		// If the pod was removed or if it was already scheduled, don't nominate it.
		updatedPod, err := npm.podLister.Pods(pi.Pod.Namespace).Get(pi.Pod.Name)
		if err != nil {
			logger.V(4).Info("Pod doesn't exist in podLister, aborted adding it to the nominator", "pod", klog.KObj(pi.Pod))
			return
		}
		if updatedPod.Spec.NodeName != "" {
			logger.V(4).Info("Pod is already scheduled to a node, aborted adding it to the nominator", "pod", klog.KObj(pi.Pod), "node", updatedPod.Spec.NodeName)
			return
		}
	}

	npm.nominatedPodToNode[pi.Pod.UID] = nodeName
	for _, np := range npm.nominatedPods[nodeName] {
		if np.uid == pi.Pod.UID {
			logger.V(4).Info("Pod already exists in the nominator", "pod", np.uid)
			return
		}
	}
	npm.nominatedPods[nodeName] = append(npm.nominatedPods[nodeName], podToRef(pi.Pod))
}

// UpdateNominatedPod updates the <oldPod> with <newPod>.
func (npm *nominator) UpdateNominatedPod(logger klog.Logger, oldPod *v1.Pod, newPodInfo *utils.PodInfo) {
	npm.nLock.Lock()
	defer npm.nLock.Unlock()
	// In some cases, an Update event with no "NominatedNode" present is received right
	// after a node("NominatedNode") is reserved for this pod in memory.
	// In this case, we need to keep reserving the NominatedNode when updating the pod pointer.
	var nominatingInfo *utils.NominatingInfo
	// We won't fall into below `if` block if the Update event represents:
	// (1) NominatedNode info is added
	// (2) NominatedNode info is updated
	// (3) NominatedNode info is removed
	if nominatedNodeName(oldPod) == "" && nominatedNodeName(newPodInfo.Pod) == "" {
		if nnn, ok := npm.nominatedPodToNode[oldPod.UID]; ok {
			// This is the only case we should continue reserving the NominatedNode
			nominatingInfo = &utils.NominatingInfo{
				NominatingMode:    utils.ModeOverride,
				NominatedNodeName: nnn,
			}
		}
	}
	// We update irrespective of the nominatedNodeName changed or not, to ensure
	// that pod pointer is updated.
	npm.deleteUnlocked(oldPod)
	npm.addNominatedPodUnlocked(logger, newPodInfo, nominatingInfo)
}

// DeleteNominatedPodIfExists deletes <pod> from nominatedPods.
func (npm *nominator) DeleteNominatedPodIfExists(pod *v1.Pod) {
	npm.nLock.Lock()
	npm.deleteUnlocked(pod)
	npm.nLock.Unlock()
}

func (npm *nominator) deleteUnlocked(p *v1.Pod) {
	nnn, ok := npm.nominatedPodToNode[p.UID]
	if !ok {
		return
	}
	for i, np := range npm.nominatedPods[nnn] {
		if np.uid == p.UID {
			npm.nominatedPods[nnn] = append(npm.nominatedPods[nnn][:i], npm.nominatedPods[nnn][i+1:]...)
			if len(npm.nominatedPods[nnn]) == 0 {
				delete(npm.nominatedPods, nnn)
			}
			break
		}
	}
	delete(npm.nominatedPodToNode, p.UID)
}

func (npm *nominator) nominatedPodsForNode(nodeName string) []podRef {
	npm.nLock.RLock()
	defer npm.nLock.RUnlock()
	return slices.Clone(npm.nominatedPods[nodeName])
}

// nominatedNodeName returns nominated node name of a Pod.
func nominatedNodeName(pod *v1.Pod) string {
	return pod.Status.NominatedNodeName
}

type podRef struct {
	name      string
	namespace string
	uid       types.UID
}

func podToRef(pod *v1.Pod) podRef {
	return podRef{
		name:      pod.Name,
		namespace: pod.Namespace,
		uid:       pod.UID,
	}
}

func (np podRef) toPod() *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      np.name,
			Namespace: np.namespace,
			UID:       np.uid,
		},
	}
}
