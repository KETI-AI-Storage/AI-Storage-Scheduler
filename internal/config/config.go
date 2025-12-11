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

	// Initialize Filter plugins
	// These plugins filter out unsuitable nodes
	filterPlugins := []framework.FilterPlugin{
		plugin.NewNodeName(),             // Checks if pod.spec.nodeName matches
		plugin.NewNodeUnschedulable(),    // Filters unschedulable nodes
		plugin.NewTaintToleration(),      // Checks taints and tolerations
		plugin.NewNodeAffinity(),         // Checks node affinity and selectors
		plugin.NewNodeResourcesFit(),     // Checks CPU, Memory, GPU resources
	}

	// Initialize Score plugins
	// These plugins rank the remaining feasible nodes
	// Plugins are weighted during scoring (default weight: 1)
	scorePlugins := []framework.ScorePlugin{
		plugin.NewLeastAllocated(cache),      // Favors nodes with more available resources (weight: 1)
		plugin.NewBalancedAllocation(cache),  // Favors balanced CPU/Memory/GPU usage (weight: 1)
		plugin.NewImageLocality(cache),       // Favors nodes with images present (weight: 1)
	}

	// Initialize Bind plugin
	bindPlugin := plugin.NewDefaultBinder(hostKubeClient)

	// Create framework
	fwk := framework.NewFramework(filterPlugins, scorePlugins, bindPlugin)

	return &SchedulerConfig{
		InformerFactory: informerFactory,
		HostKubeClient:  hostKubeClient,
		Framework:       fwk,
		Cache:           cache,
	}
}
