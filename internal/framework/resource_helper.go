package framework

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// /kubernetes/staging/src/k8s.io/component-helpers/resource/helpers.go

type ContainerType int

const (
	Containers ContainerType = 1 << iota
	InitContainers
)

type PodResourcesOptions struct {
	Reuse                       v1.ResourceList
	UseStatusResources          bool
	ExcludeOverhead             bool
	ContainerFn                 func(res v1.ResourceList, containerType ContainerType)
	NonMissingContainerRequests v1.ResourceList
	SkipPodLevelResources       bool
}

var supportedPodLevelResources = sets.New(v1.ResourceCPU, v1.ResourceMemory)

func SupportedPodLevelResources() sets.Set[v1.ResourceName] {
	return supportedPodLevelResources
}

func IsSupportedPodLevelResource(name v1.ResourceName) bool {
	return supportedPodLevelResources.Has(name)
}

func IsPodLevelResourcesSet(pod *v1.Pod) bool {
	if pod.Spec.Resources == nil {
		return false
	}

	if (len(pod.Spec.Resources.Requests) + len(pod.Spec.Resources.Limits)) == 0 {
		return false
	}

	for resourceName := range pod.Spec.Resources.Requests {
		if IsSupportedPodLevelResource(resourceName) {
			return true
		}
	}

	for resourceName := range pod.Spec.Resources.Limits {
		if IsSupportedPodLevelResource(resourceName) {
			return true
		}
	}

	return false
}

func IsPodLevelRequestsSet(pod *v1.Pod) bool {
	if pod.Spec.Resources == nil {
		return false
	}

	if len(pod.Spec.Resources.Requests) == 0 {
		return false
	}

	for resourceName := range pod.Spec.Resources.Requests {
		if IsSupportedPodLevelResource(resourceName) {
			return true
		}
	}

	return false
}

func PodRequests(pod *v1.Pod, opts PodResourcesOptions) v1.ResourceList {
	reqs := AggregateContainerRequests(pod, opts)
	if !opts.SkipPodLevelResources && IsPodLevelRequestsSet(pod) {
		for resourceName, quantity := range pod.Spec.Resources.Requests {
			if IsSupportedPodLevelResource(resourceName) {
				reqs[resourceName] = quantity
			}
		}
	}

	// Add overhead for running a pod to the sum of requests if requested:
	if !opts.ExcludeOverhead && pod.Spec.Overhead != nil {
		addResourceList(reqs, pod.Spec.Overhead)
	}

	return reqs
}

func AggregateContainerRequests(pod *v1.Pod, opts PodResourcesOptions) v1.ResourceList {
	reqs := reuseOrClearResourceList(opts.Reuse)
	var containerStatuses map[string]*v1.ContainerStatus
	if opts.UseStatusResources {
		containerStatuses = make(map[string]*v1.ContainerStatus, len(pod.Status.ContainerStatuses))
		for i := range pod.Status.ContainerStatuses {
			containerStatuses[pod.Status.ContainerStatuses[i].Name] = &pod.Status.ContainerStatuses[i]
		}
	}

	for _, container := range pod.Spec.Containers {
		containerReqs := container.Resources.Requests
		if opts.UseStatusResources {
			cs, found := containerStatuses[container.Name]
			if found && cs.Resources != nil {
				if pod.Status.Resize == v1.PodResizeStatusInfeasible {
					containerReqs = cs.Resources.Requests.DeepCopy()
				} else {
					containerReqs = max(container.Resources.Requests, cs.Resources.Requests)
				}
			}
		}

		if len(opts.NonMissingContainerRequests) > 0 {
			containerReqs = applyNonMissing(containerReqs, opts.NonMissingContainerRequests)
		}

		if opts.ContainerFn != nil {
			opts.ContainerFn(containerReqs, Containers)
		}

		addResourceList(reqs, containerReqs)
	}

	restartableInitContainerReqs := v1.ResourceList{}
	initContainerReqs := v1.ResourceList{}

	for _, container := range pod.Spec.InitContainers {
		containerReqs := container.Resources.Requests
		if len(opts.NonMissingContainerRequests) > 0 {
			containerReqs = applyNonMissing(containerReqs, opts.NonMissingContainerRequests)
		}

		if container.RestartPolicy != nil && *container.RestartPolicy == v1.ContainerRestartPolicyAlways {
			addResourceList(reqs, containerReqs)

			addResourceList(restartableInitContainerReqs, containerReqs)
			containerReqs = restartableInitContainerReqs
		} else {
			tmp := v1.ResourceList{}
			addResourceList(tmp, containerReqs)
			addResourceList(tmp, restartableInitContainerReqs)
			containerReqs = tmp
		}

		if opts.ContainerFn != nil {
			opts.ContainerFn(containerReqs, InitContainers)
		}
		maxResourceList(initContainerReqs, containerReqs)
	}

	maxResourceList(reqs, initContainerReqs)
	return reqs
}

func applyNonMissing(reqs v1.ResourceList, nonMissing v1.ResourceList) v1.ResourceList {
	cp := v1.ResourceList{}
	for k, v := range reqs {
		cp[k] = v.DeepCopy()
	}

	for k, v := range nonMissing {
		if _, found := reqs[k]; !found {
			rk := cp[k]
			rk.Add(v)
			cp[k] = rk
		}
	}
	return cp
}

func PodLimits(pod *v1.Pod, opts PodResourcesOptions) v1.ResourceList {
	// attempt to reuse the maps if passed, or allocate otherwise
	limits := AggregateContainerLimits(pod, opts)
	if !opts.SkipPodLevelResources && IsPodLevelResourcesSet(pod) {
		for resourceName, quantity := range pod.Spec.Resources.Limits {
			if IsSupportedPodLevelResource(resourceName) {
				limits[resourceName] = quantity
			}
		}
	}

	// Add overhead to non-zero limits if requested:
	if !opts.ExcludeOverhead && pod.Spec.Overhead != nil {
		for name, quantity := range pod.Spec.Overhead {
			if value, ok := limits[name]; ok && !value.IsZero() {
				value.Add(quantity)
				limits[name] = value
			}
		}
	}

	return limits
}

func AggregateContainerLimits(pod *v1.Pod, opts PodResourcesOptions) v1.ResourceList {
	limits := reuseOrClearResourceList(opts.Reuse)
	var containerStatuses map[string]*v1.ContainerStatus
	if opts.UseStatusResources {
		containerStatuses = make(map[string]*v1.ContainerStatus, len(pod.Status.ContainerStatuses))
		for i := range pod.Status.ContainerStatuses {
			containerStatuses[pod.Status.ContainerStatuses[i].Name] = &pod.Status.ContainerStatuses[i]
		}
	}

	for _, container := range pod.Spec.Containers {
		containerLimits := container.Resources.Limits
		if opts.UseStatusResources {
			cs, found := containerStatuses[container.Name]
			if found && cs.Resources != nil {
				if pod.Status.Resize == v1.PodResizeStatusInfeasible {
					containerLimits = cs.Resources.Limits.DeepCopy()
				} else {
					containerLimits = max(container.Resources.Limits, cs.Resources.Limits)
				}
			}
		}

		if opts.ContainerFn != nil {
			opts.ContainerFn(containerLimits, Containers)
		}
		addResourceList(limits, containerLimits)
	}

	restartableInitContainerLimits := v1.ResourceList{}
	initContainerLimits := v1.ResourceList{}

	for _, container := range pod.Spec.InitContainers {
		containerLimits := container.Resources.Limits
		if container.RestartPolicy != nil && *container.RestartPolicy == v1.ContainerRestartPolicyAlways {
			addResourceList(limits, containerLimits)

			addResourceList(restartableInitContainerLimits, containerLimits)
			containerLimits = restartableInitContainerLimits
		} else {
			tmp := v1.ResourceList{}
			addResourceList(tmp, containerLimits)
			addResourceList(tmp, restartableInitContainerLimits)
			containerLimits = tmp
		}

		if opts.ContainerFn != nil {
			opts.ContainerFn(containerLimits, InitContainers)
		}
		maxResourceList(initContainerLimits, containerLimits)
	}

	maxResourceList(limits, initContainerLimits)
	return limits
}

func addResourceList(list, newList v1.ResourceList) {
	for name, quantity := range newList {
		if value, ok := list[name]; !ok {
			list[name] = quantity.DeepCopy()
		} else {
			value.Add(quantity)
			list[name] = value
		}
	}
}

func maxResourceList(list, newList v1.ResourceList) {
	for name, quantity := range newList {
		if value, ok := list[name]; !ok || quantity.Cmp(value) > 0 {
			list[name] = quantity.DeepCopy()
		}
	}
}

func max(a v1.ResourceList, b v1.ResourceList) v1.ResourceList {
	result := v1.ResourceList{}
	for key, value := range a {
		if other, found := b[key]; found {
			if value.Cmp(other) <= 0 {
				result[key] = other.DeepCopy()
				continue
			}
		}
		result[key] = value.DeepCopy()
	}
	for key, value := range b {
		if _, found := result[key]; !found {
			result[key] = value.DeepCopy()
		}
	}
	return result
}

func reuseOrClearResourceList(reuse v1.ResourceList) v1.ResourceList {
	if reuse == nil {
		return make(v1.ResourceList, 4)
	}
	for k := range reuse {
		delete(reuse, k)
	}
	return reuse
}
