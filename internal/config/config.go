package config

import (
	"context"
	"os"

	logger "keti/ai-storage-scheduler/internal/backend/log"
	"keti/ai-storage-scheduler/internal/configmanager"
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

// getPluginWeight returns the weight for a KETI plugin from CRD config
func getPluginWeight(pluginName string) int32 {
	cfg := configmanager.GetManager().GetPluginConfig()

	switch pluginName {
	case "DataLocalityAware":
		if cfg.DataLocalityAware.Weight > 0 {
			return int32(cfg.DataLocalityAware.Weight)
		}
		return 3 // default
	case "StorageTierAware":
		if cfg.StorageTierAware.Weight > 0 {
			return int32(cfg.StorageTierAware.Weight)
		}
		return 3 // default
	case "IOPatternBased":
		if cfg.IOPatternBased.Weight > 0 {
			return int32(cfg.IOPatternBased.Weight)
		}
		return 3 // default
	case "KueueAware":
		if cfg.KueueAware.Weight > 0 {
			return int32(cfg.KueueAware.Weight)
		}
		return 2 // default
	case "PipelineStageAware":
		if cfg.PipelineStageAware.Weight > 0 {
			return int32(cfg.PipelineStageAware.Weight)
		}
		return 3 // default
	case "CSIStorageAware":
		if cfg.CSIStorageAware.Weight > 0 {
			return int32(cfg.CSIStorageAware.Weight)
		}
		return 2 // default
	default:
		return 1
	}
}

func CreateDefaultConfig() *SchedulerConfig {
	hostConfig, err := rest.InClusterConfig()
	if err != nil {
		logger.Error("Failed to get cluster config", err)
		os.Exit(1)
	}
	hostKubeClient := kubernetes.NewForConfigOrDie(hostConfig)

	// Initialize ConfigManager for dynamic CRD-based configuration
	cfgMgr := configmanager.GetManager()
	if err := cfgMgr.Initialize(hostConfig); err != nil {
		logger.Info("ConfigManager initialization warning (using defaults)", "error", err)
	} else {
		logger.Info("ConfigManager initialized successfully")
	}

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

		// ============================================================
		// KETI AI Storage Preprocessing Plugins (Filter)
		// ============================================================
		plugin.NewDataLocalityAware(cache, hostKubeClient), // Filters nodes without PVC access
		plugin.NewStorageTierAware(cache, hostKubeClient),  // Filters nodes without required storage tier
		plugin.NewIOPatternBased(cache, hostKubeClient),    // Filters nodes without CSD for CSD-required workloads
		plugin.NewCSIStorageAware(cache, hostKubeClient),   // Filters nodes without required CSI driver or capacity
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

		// ============================================================
		// KETI AI Storage Preprocessing Plugins (Score)
		// Weights are loaded from CRD AIStorageConfig
		// ============================================================

		// DataLocalityAware: Prefers nodes with data locality for preprocessing
		// - APOLLO node preference (0-30 points, configurable)
		// - PVC locality scoring (0-30 points, configurable)
		// - Data cache presence (0-20 points, configurable)
		// - Network topology proximity (0-20 points, configurable)
		{Plugin: plugin.NewDataLocalityAware(cache, hostKubeClient), Weight: getPluginWeight("DataLocalityAware")},

		// StorageTierAware: Prefers nodes with optimal storage tier for I/O pattern
		// - I/O pattern matching (0-40 points, configurable)
		// - Pipeline stage optimization (0-30 points, configurable)
		// - IOPS/throughput requirements (0-20 points, configurable)
		// - Storage capacity (0-10 points, configurable)
		{Plugin: plugin.NewStorageTierAware(cache, hostKubeClient), Weight: getPluginWeight("StorageTierAware")},

		// IOPatternBased: Prefers nodes optimized for preprocessing type
		// - APOLLO preference (0-15 points, configurable)
		// - Resource requirements match (0-25 points, configurable)
		// - I/O optimization (0-20 points, configurable)
		// - Data expansion handling (0-20 points, configurable)
		// - CSD offload capability (0-20 points, configurable)
		{Plugin: plugin.NewIOPatternBased(cache, hostKubeClient), Weight: getPluginWeight("IOPatternBased")},

		// KueueAware: Prefers nodes optimized for Kueue-managed workloads
		// - Gang locality (0-30 points, configurable) - 같은 Gang 멤버 근접 배치
		// - Queue priority (0-20 points, configurable) - Queue 우선순위 기반
		// - Workload size (0-25 points, configurable) - 대규모 워크로드 여유 노드 선호
		// - Network proximity (0-25 points, configurable) - Gang 멤버 네트워크 근접성
		{Plugin: plugin.NewKueueAware(cache, hostKubeClient), Weight: getPluginWeight("KueueAware")},

		// PipelineStageAware: Prefers nodes for pipeline stage data locality
		// - Dependency locality (0-40 points, configurable) - 이전 스테이지 실행 노드 선호
		// - Pipeline cohesion (0-30 points, configurable) - 같은 파이프라인 스텝 노드 선호
		// - I/O pattern (0-30 points, configurable) - 워크로드 타입에 따른 노드 선택
		{Plugin: plugin.NewPipelineStageAware(cache, hostKubeClient), Weight: getPluginWeight("PipelineStageAware")},

		// CSIStorageAware: Prefers nodes with CSI storage capacity and driver availability
		// - CSI storage capacity (0-40 points) - CSIStorageCapacity 가용 용량 기반
		// - Volume headroom (0-30 points) - CSINode 드라이버 볼륨 여유분
		// - Topology matching (0-30 points) - CSI 토폴로지 키 매칭
		// 조건부: Pod에 PVC가 없으면 중립 점수(50)
		{Plugin: plugin.NewCSIStorageAware(cache, hostKubeClient), Weight: getPluginWeight("CSIStorageAware")},
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

	// Log each score plugin for debugging
	for i, pw := range scorePlugins {
		logger.Info("[Score Plugin registered]",
			"index", i,
			"name", pw.Plugin.Name(),
			"weight", pw.Weight)
	}

	return &SchedulerConfig{
		InformerFactory: informerFactory,
		HostKubeClient:  hostKubeClient,
		Framework:       fwk,
		Cache:           cache,
	}
}
