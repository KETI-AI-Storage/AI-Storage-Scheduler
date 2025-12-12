package scheduler

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	logger "keti/ai-storage-scheduler/internal/backend/log"
)

func AddAllEventHandlers(
	sched *Scheduler,
	informerFactory informers.SharedInformerFactory,
) error {
	// scheduled pod -> cache
	informerFactory.Core().V1().Pods().Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj any) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					return assignedPod(t) /* && sched.Cache.NodeInfoExist(t)*/
				case cache.DeletedFinalStateUnknown:
					if _, ok := t.Obj.(*v1.Pod); ok {
						return true
					}
					return false
				default:
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
			FilterFunc: func(obj any) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					return !assignedPod(t) && responsibleForPod(t)
				case cache.DeletedFinalStateUnknown:
					return false
				default:
					logger.Warn("[error] unable to handle object", "type", fmt.Sprintf("%T", obj))
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
			FilterFunc: func(obj any) bool {
				switch obj.(type) {
				case *v1.Node:
					return true
				case cache.DeletedFinalStateUnknown:
					return false
				default:
					logger.Warn("[error] unable to handle object", "type", fmt.Sprintf("%T", obj))
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

	return nil
}

func (sched *Scheduler) addNodeToCache(obj any) {
	node, ok := obj.(*v1.Node)
	if !ok {
		logger.Warn("[error] cannot convert to *v1.Node", "obj", obj)
		return
	}

	logger.Info("[event] add new node to cache", "node", node.Name)

	err := sched.Cache.AddNode(node, sched.schedulerConfig.HostKubeClient)
	if err != nil {
		logger.Error("[error] cannot add node to cache", err, "node", node.Name)
	}

	// Fetch GPU metrics for new node (async)
	go sched.fetchNodeGPUMetrics(node.Name)

	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue()
}

func (sched *Scheduler) updateNodeInCache(oldObj, newObj any) {
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

	err := sched.Cache.UpdateNode(oldNode, newNode)
	if err != nil {
		logger.Error("[error] cannot update node in cache", err, "node", newNode.Name)
	}

	// Update GPU metrics for changed node (async)
	go sched.fetchNodeGPUMetrics(newNode.Name)

	// Only requeue unschedulable pods if the node became more schedulable.
	event := NodeSchedulingPropertiesChange(newNode, oldNode)
	if event != nil {
		// if event == &NodeAllocatableChange {
		// 	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue() //flush only unschedulable pod
		// } else {
		// 	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue() //flush all pods
		// }
	}
}

func (sched *Scheduler) deleteNodeFromCache(obj any) {
	var node *v1.Node
	switch t := obj.(type) {
	case *v1.Node:
		node = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		node, ok = t.Obj.(*v1.Node)
		if !ok {
			return
		}
	default:
		return
	}

	if err := sched.Cache.RemoveNode(node); err != nil {
		logger.Error("[error] scheduler cache RemoveNode failed", err, "node", node.Name)
	}
}

func (sched *Scheduler) addPodToSchedulingQueue(obj any) {
	pod := obj.(*v1.Pod)
	gpuRequest := "none"
	if len(pod.Spec.Containers) > 0 {
		if gpu, ok := pod.Spec.Containers[0].Resources.Requests["nvidia.com/gpu"]; ok {
			gpuRequest = gpu.String()
		}
	}
	logger.Info("[event] add pod to scheduling queue",
		"namespace", pod.Namespace, "pod", pod.Name, "gpu", gpuRequest)
	sched.SchedulingQueue.Add(pod)
}

func (sched *Scheduler) updatePodInSchedulingQueue(oldObj, newObj any) {
	oldPod, newPod := oldObj.(*v1.Pod), newObj.(*v1.Pod)
	if oldPod.ResourceVersion == newPod.ResourceVersion {
		return
	}

	isAssumed, err := sched.Cache.IsAssumedPod(newPod)
	if err != nil {
		logger.Warn("[error] failed to check whether pod is assumed", 
			"namespace", newPod.Namespace, "pod", newPod.Name, "error", err.Error())
	}
	if isAssumed {
		return
	}

	if err := sched.SchedulingQueue.Update(oldPod, newPod); err != nil {
		logger.Warn("[error] unable to update pod in scheduling queue", 
			"namespace", newPod.Namespace, "pod", newPod.Name, "error", err.Error())
	}
}

func (sched *Scheduler) deletePodFromSchedulingQueue(obj any) {
	var pod *v1.Pod
	switch t := obj.(type) {
	case *v1.Pod:
		pod = obj.(*v1.Pod)
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pod, ok = t.Obj.(*v1.Pod)
		if !ok {
			logger.Warn("[error] unable to convert object to *v1.Pod", "obj", obj)
			return
		}
	default:
		logger.Warn("[error] unable to handle object", "type", fmt.Sprintf("%T", obj))
		return
	}

	if err := sched.SchedulingQueue.Delete(pod); err != nil {
		logger.Warn("[error] unable to dequeue pod", 
			"namespace", pod.Namespace, "pod", pod.Name, "error", err.Error())
	}
}

func (sched *Scheduler) addPodToCache(obj any) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return
	}

	if err := sched.Cache.AddPod(pod); err != nil {
		logger.Error("[error] scheduler cache AddPod failed", err, 
			"namespace", pod.Namespace, "pod", pod.Name)
	}
}

func (sched *Scheduler) updatePodInCache(oldObj, newObj any) {
	oldPod, ok := oldObj.(*v1.Pod)
	if !ok {
		return
	}
	newPod, ok := newObj.(*v1.Pod)
	if !ok {
		return
	}

	if err := sched.Cache.UpdatePod(oldPod, newPod); err != nil {
		logger.Error("[error] scheduler cache UpdatePod failed", err,
			"namespace", oldPod.Namespace, "pod", oldPod.Name)
	}
}

func (sched *Scheduler) deletePodFromCache(obj any) {
	var pod *v1.Pod
	switch t := obj.(type) {
	case *v1.Pod:
		pod = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pod, ok = t.Obj.(*v1.Pod)
		if !ok {
			logger.Warn("[error] cannot convert to *v1.Pod", "obj", t.Obj)
			return
		}
	default:
		logger.Warn("[error] cannot convert to *v1.Pod", "obj", t)
		return
	}

	logger.Info("[event] delete pod from cache", "namespace", pod.Namespace, "pod", pod.Name)
	if err := sched.Cache.RemovePod(pod); err != nil {
		klog.ErrorS(err, "[error] scheduler cache remove pod failed", "pod", klog.KObj(pod))
	}
}

// assignedPod selects pods that are assigned (scheduled and running).
func assignedPod(pod *v1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}

// responsibleForPod returns true if the pod has asked to be scheduled by the given scheduler.
func responsibleForPod(pod *v1.Pod) bool {
	responsibleForPod := (pod.Spec.SchedulerName == "ai-storage-scheduler")
	return responsibleForPod
}
