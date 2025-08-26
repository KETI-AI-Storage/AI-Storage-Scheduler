package scheduler

import (
	"fmt"
	"reflect"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	logger "keti/ai-storage-scheduler/internal/log"
)

func AddAllEventHandlers(
	sched *Scheduler,
	informerFactory informers.SharedInformerFactory,
) error {
	// scheduled pod -> cache
	informerFactory.Core().V1().Pods().Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					return assignedPod(t) && sched.nodeInfoExist(t)
				case cache.DeletedFinalStateUnknown:
					if _, ok := t.Obj.(*v1.Pod); ok {
						// The carried object may be stale, so we don't use it to check if
						// it's assigned or not. Attempting to cleanup anyways.
						return true
					}
					logger.Warn(fmt.Sprintf("[error] unable to convert object %T to *v1.Pod in %T\n", obj, sched))
					return false
				default:
					logger.Warn(fmt.Sprintf("[error] unable to handle object in %T: %T\n", sched, obj))
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    sched.addPodToCache,
				UpdateFunc: sched.updatePodInCache,
				DeleteFunc: sched.deletePodFromCache,
			},
		},
	)

	// unscheduled pod -> scheduling queue
	informerFactory.Core().V1().Pods().Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					return !assignedPod(t) && responsibleForPod(t)
				case cache.DeletedFinalStateUnknown:
					return false
				default:
					logger.Warn(fmt.Sprintf("[error] unable to handle object in %T: %T\n", sched, obj))
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    sched.addPodToSchedulingQueue,
				UpdateFunc: sched.updatePodInSchedulingQueue,
				DeleteFunc: sched.deletePodFromSchedulingQueue,
			},
		},
	)

	// node
	informerFactory.Core().V1().Nodes().Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch obj.(type) {
				case *v1.Node:
					return true //!r.IsMasterNode(t)
				case cache.DeletedFinalStateUnknown:
					return false
				default:
					logger.Warn(fmt.Sprintf("[error] unable to handle object in %T: %T\n", sched, obj))
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    sched.addNodeToCache,
				UpdateFunc: sched.updateNodeInCache,
				DeleteFunc: sched.deleteNodeFromCache,
			},
		},
	)
}

func (sched *Scheduler) nodeInfoExist(pod *v1.Pod) bool {
	if _, ok := sched.NodeInfoCache.NodeInfoList[pod.Spec.NodeName]; ok {
		return true
	}
	return false
}

func (sched *Scheduler) addNodeToCache(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		logger.Warn(fmt.Sprintf("[error] cannot convert to *v1.Node -> %+v", obj))
		return
	}

	if _, ok := sched.NodeInfoCache.NodeInfoList[node.Name]; ok {
		return
	}

	logger.Info(fmt.Sprintf("[event] add new node {%s} to cache\n", node.Name))

	err := sched.NodeInfoCache.AddNode(node)
	if err != nil {
		klog.ErrorS(nil, "cannot add node [", node.Name, "]")
	}

	// klog.V(3).InfoS("Add event for node", "node", klog.KObj(node))
	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(true)
}

func (sched *Scheduler) updateNodeInCache(oldObj, newObj interface{}) {
	oldNode, ok := oldObj.(*v1.Node)
	if !ok {
		klog.ErrorS(nil, "cannot convert oldObj to *v1.Node", "oldObj", oldObj)
		return
	}

	newNode, ok := newObj.(*v1.Node)
	if !ok {
		klog.ErrorS(nil, "cannot convert newObj to *v1.Node", "newObj", newObj)
		return
	}

	if _, ok := sched.NodeInfoCache.NodeInfoList[newNode.Name]; !ok {
		return
	}

	err := sched.NodeInfoCache.UpdateNode(oldNode, newNode)
	if err != nil {
		klog.ErrorS(nil, "cannot Update Node [", newNode.Name, "]")
	}

	// Only requeue unschedulable pods if the node became more schedulable.
	event := nodeSchedulingPropertiesChange(newNode, oldNode)
	if event != nil {
		if event == &NodeAllocatableChange {
			sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(false) //flush only unschedulable pod
		} else {
			sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(true) //flush all pods
		}
	}
}

func (sched *Scheduler) deleteNodeFromCache(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		klog.ErrorS(nil, "cannot convert newObj to *v1.Node", "newObj", obj)
		return
	}

	if _, ok := sched.NodeInfoCache.NodeInfoList[node.Name]; !ok {
		return
	}

	if err := sched.NodeInfoCache.RemoveNode(node); err != nil {
		klog.ErrorS(err, "scheduler cache remove node failed")
	}
}

func (sched *Scheduler) addPodToSchedulingQueue(obj interface{}) {
	pod := obj.(*v1.Pod)
	sched.SchedulingQueue.Add(pod)
	sched.NodeInfoCache.AddPodState(*pod, r.Pending)
}

func (sched *Scheduler) updatePodInSchedulingQueue(oldObj, newObj interface{}) {
	oldPod, newPod := oldObj.(*v1.Pod), newObj.(*v1.Pod)
	if oldPod.ResourceVersion == newPod.ResourceVersion {
		return
	}

	if ok, state := sched.NodeInfoCache.CheckPodStateExist(newPod); ok {
		if state != r.Pending {
			return
		}
	}

	if err := sched.SchedulingQueue.Update(oldPod, newPod); err != nil {
		logger.Warn(fmt.Sprintf("[error] unable to update %T: %v\n", newObj, err))
	}
}

func (sched *Scheduler) deletePodFromSchedulingQueue(obj interface{}) {
	var pod *v1.Pod
	switch t := obj.(type) {
	case *v1.Pod:
		pod = obj.(*v1.Pod)
		// fmt.Println("deletePodFromSchedulingQueue: ", pod.Name)
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pod, ok = t.Obj.(*v1.Pod)
		if !ok {
			logger.Warn(fmt.Sprintf("[error] unable to convert object %T to *v1.Pod in %T\n", obj, sched))
			return
		}
	default:
		logger.Warn(fmt.Sprintf("[error] unable to handle object in %T: %T\n", sched, obj))
		return
	}

	if ok, state := sched.NodeInfoCache.CheckPodStateExist(pod); ok {
		if state != r.Pending {
			return
		}
	}

	if err := sched.SchedulingQueue.Delete(pod); err != nil {
		logger.Warn(fmt.Sprintf("[error] unable to dequeue %T: %v\n", obj, err))
	}
}

func (sched *Scheduler) addPodToCache(obj interface{}) {
	pod, _ := obj.(*v1.Pod)

	if ok, state := sched.NodeInfoCache.CheckPodStateExist(pod); ok { // scheduled pod already in cache
		if state == r.BindingFinished {
			return

		} else if state == r.Pending {
			pod := obj.(*v1.Pod)
			logger.Warn(fmt.Sprintf("[error] pod {%s} state is pending", pod.Name)) // penging pod cannot add to cache
			return
		}
	}

	// Assumed Pod or scehduled other scheduler
	sched.NodeInfoCache.AddPod(pod, r.BindingFinished)
}

func (sched *Scheduler) updatePodInCache(oldObj, newObj interface{}) {
	oldPod, ok := oldObj.(*v1.Pod)
	if !ok {
		logger.Warn(fmt.Sprintf("[error] cannot update pod %s in cache\n", oldPod.Name))
		return
	}

	// newPod, ok := newObj.(*v1.Pod)
	_, ok = newObj.(*v1.Pod)
	if !ok {
		logger.Warn(fmt.Sprintf("[error] cannot update pod %s in cache\n", oldPod.Name))
		return
	}

}

func (sched *Scheduler) deletePodFromCache(obj interface{}) {
	var pod *v1.Pod
	switch t := obj.(type) {
	case *v1.Pod:
		pod = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pod, ok = t.Obj.(*v1.Pod)
		if !ok {
			logger.Warn(fmt.Sprintf("cannot convert to *v1.Pod -> %+v", t.Obj))
			return
		}
	default:
		logger.Warn(fmt.Sprintf("cannot convert to *v1.Pod -> %+v", t))
		return
	}

	if ok, _ := sched.NodeInfoCache.CheckPodStateExist(pod); !ok {
		logger.Warn(fmt.Sprintf("[error] cannot delete. there isn't pod {%s} state", pod.Name))
		return
	}

	logger.Info(fmt.Sprintf("[event] delete pod {%s} from cache\n", pod.Name))
	if err := sched.NodeInfoCache.RemovePod(pod); err != nil {
		klog.ErrorS(err, "[error] scheduler cache remove pod failed", "pod", klog.KObj(pod))
	}
}

// assignedPod selects pods that are assigned (scheduled and running).
func assignedPod(pod *v1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}

// responsibleForPod returns true if the pod has asked to be scheduled by the given scheduler.
func responsibleForPod(pod *v1.Pod) bool {
	responsibleForPod := (pod.Spec.SchedulerName == "ai-storage-sheduler")
	return responsibleForPod
}

func nodeSchedulingPropertiesChange(newNode *v1.Node, oldNode *v1.Node) *r.ClusterEvent {
	if nodeSpecUnschedulableChanged(newNode, oldNode) {
		return &NodeSpecUnschedulableChange
	}
	if nodeAllocatableChanged(newNode, oldNode) {
		return &NodeAllocatableChange
	}
	if nodeLabelsChanged(newNode, oldNode) {
		return &NodeLabelChange
	}
	if nodeTaintsChanged(newNode, oldNode) {
		return &NodeTaintChange
	}
	if nodeConditionsChanged(newNode, oldNode) {
		return &NodeConditionChange
	}

	return nil
}

func nodeAllocatableChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	return !reflect.DeepEqual(oldNode.Status.Allocatable, newNode.Status.Allocatable)
}

func nodeLabelsChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	return !reflect.DeepEqual(oldNode.GetLabels(), newNode.GetLabels())
}

func nodeTaintsChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	return !reflect.DeepEqual(newNode.Spec.Taints, oldNode.Spec.Taints)
}

func nodeConditionsChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	strip := func(conditions []v1.NodeCondition) map[v1.NodeConditionType]v1.ConditionStatus {
		conditionStatuses := make(map[v1.NodeConditionType]v1.ConditionStatus, len(conditions))
		for i := range conditions {
			conditionStatuses[conditions[i].Type] = conditions[i].Status
		}
		return conditionStatuses
	}
	return !reflect.DeepEqual(strip(oldNode.Status.Conditions), strip(newNode.Status.Conditions))
}

func nodeSpecUnschedulableChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	return newNode.Spec.Unschedulable != oldNode.Spec.Unschedulable && !newNode.Spec.Unschedulable
}
