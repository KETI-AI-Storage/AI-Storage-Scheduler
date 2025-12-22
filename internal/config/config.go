package config

import (
	"context"
	"os"

	logger "keti/ai-storage-scheduler/internal/backend/log"
	framework "keti/ai-storage-scheduler/internal/framework"
	"keti/ai-storage-scheduler/internal/framework/plugin"
	"keti/ai-storage-scheduler/internal/framework/utils"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type SchedulerConfig struct {
	HostKubeClient  *kubernetes.Clientset
	InformerFactory informers.SharedInformerFactory
	Framework       framework.Framework
	Cache           *utils.Cache
}

func CreateDefaultConfig() *SchedulerConfig {
	hostConfig, err := rest.InClusterConfig()
	if err != nil {
		logger.Error("Failed to get cluster config", err)
		os.Exit(1)
	}
	hostKubeClient := kubernetes.NewForConfigOrDie(hostConfig)

	informerFactory := informers.NewSharedInformerFactory(hostKubeClient, 0)

	// Initialize cache (needed for score plugins)
	ctx := context.Background()
	cache := utils.NewCache(ctx)

	// ============================================================
	// K8s v1.32 Default Scheduler Plugins
	// ============================================================

	// PreFilter plugins
	// These run before filtering to precompute information
	preFilterPlugins := []framework.PreFilterPlugin{
		plugin.NewVolumeRestrictions(),          // Check volume restrictions
		plugin.NewInterPodAffinity(cache),       // Precompute pod affinity data
		plugin.NewPodTopologySpread(cache),      // Precompute topology spread data
		plugin.NewVolumeBinding(hostKubeClient), // Check PVC bindings
	}

	// Filter plugins
	// These filter out unsuitable nodes
	filterPlugins := []framework.FilterPlugin{
		plugin.NewNodeName(),                       // Checks if pod.spec.nodeName matches
		plugin.NewNodeUnschedulable(),              // Filters unschedulable nodes
		plugin.NewNodePorts(),                      // Checks host port conflicts
		plugin.NewTaintToleration(),                // Checks taints and tolerations
		plugin.NewNodeAffinityWithCache(cache),     // Checks node affinity and selectors
		plugin.NewNodeResourcesFit(),               // Checks CPU, Memory, GPU resources
		plugin.NewVolumeRestrictions(),             // Checks volume restrictions
		plugin.NewVolumeZone(hostKubeClient),       // Checks volume zone constraints
		plugin.NewVolumeBinding(hostKubeClient),    // Checks if volumes can be bound
		plugin.NewEBSLimits(),                      // Checks AWS EBS volume limits (max 39)
		plugin.NewGCEPDLimits(),                    // Checks GCE PD volume limits (max 16)
		plugin.NewAzureDiskLimits(),                // Checks Azure Disk volume limits (max 16)
		plugin.NewNodeVolumeLimits(hostKubeClient), // Checks CSI volume limits (max 256)
		plugin.NewInterPodAffinity(cache),          // Checks pod affinity/anti-affinity
		plugin.NewPodTopologySpread(cache),         // Checks topology spread constraints
	}

	// PostFilter plugins (for preemption)
	postFilterPlugins := []framework.PostFilterPlugin{
		plugin.NewDefaultPreemption(hostKubeClient, cache), // Default preemption mechanism
	}

	// PreScore plugins
	preScorePlugins := []framework.PreScorePlugin{
		plugin.NewInterPodAffinity(cache),  // Prepare affinity scoring data
		plugin.NewPodTopologySpread(cache), // Prepare topology spread scoring data
	}

	// Score plugins with weights (K8s v1.32 default weights)
	// Weight affects how much each plugin contributes to final score
	scorePlugins := []framework.PluginWeight{
		// TaintToleration: Prefers nodes with fewer untolerated PreferNoSchedule taints
		{Plugin: plugin.NewTaintTolerationWithCache(cache), Weight: 3},

		// NodeAffinity: Prefers nodes matching preferred scheduling terms
		{Plugin: plugin.NewNodeAffinityWithCache(cache), Weight: 2},

		// NodeResourcesFit (LeastAllocated): Prefers nodes with more available resources
		{Plugin: plugin.NewLeastAllocated(cache), Weight: 1},

		// NodeResourcesBalancedAllocation: Prefers balanced CPU/Memory usage
		{Plugin: plugin.NewBalancedAllocation(cache), Weight: 1},

		// ImageLocality: Prefers nodes with container images already present
		{Plugin: plugin.NewImageLocality(cache), Weight: 1},

		// InterPodAffinity: Prefers nodes matching preferred pod affinity
		{Plugin: plugin.NewInterPodAffinity(cache), Weight: 2},

		// PodTopologySpread: Prefers nodes that improve topology spread
		{Plugin: plugin.NewPodTopologySpread(cache), Weight: 2},

		// VolumeBinding: Prefers nodes where volumes are accessible
		{Plugin: plugin.NewVolumeBinding(hostKubeClient), Weight: 1},
	}

	// Reserve plugins
	reservePlugins := []framework.ReservePlugin{
		plugin.NewVolumeBinding(hostKubeClient), // Reserve volume bindings
	}

	// PreBind plugins
	preBindPlugins := []framework.PreBindPlugin{
		plugin.NewVolumeBinding(hostKubeClient), // Bind volumes before pod binding
	}

	// Bind plugin
	bindPlugin := plugin.NewDefaultBinder(hostKubeClient)

	// Create framework with full configuration
	fwk := framework.NewFrameworkWithConfig(&framework.FrameworkConfig{
		PreFilterPlugins:  preFilterPlugins,
		FilterPlugins:     filterPlugins,
		PostFilterPlugins: postFilterPlugins,
		PreScorePlugins:   preScorePlugins,
		ScorePlugins:      scorePlugins,
		ReservePlugins:    reservePlugins,
		PreBindPlugins:    preBindPlugins,
		BindPlugin:        bindPlugin,
	})

	logger.Info("Scheduler plugins initialized",
		"prefilter_count", len(preFilterPlugins),
		"filter_count", len(filterPlugins),
		"postfilter_count", len(postFilterPlugins),
		"prescore_count", len(preScorePlugins),
		"score_count", len(scorePlugins),
		"reserve_count", len(reservePlugins),
		"prebind_count", len(preBindPlugins))

	return &SchedulerConfig{
		InformerFactory: informerFactory,
		HostKubeClient:  hostKubeClient,
		Framework:       fwk,
		Cache:           cache,
	}
}
