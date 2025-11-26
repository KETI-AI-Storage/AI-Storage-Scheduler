package config

import (
	"os"

	logger "keti/ai-storage-scheduler/internal/backend/log"
	framework "keti/ai-storage-scheduler/internal/framework"
	"keti/ai-storage-scheduler/internal/framework/plugin"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type SchedulerConfig struct {
	HostKubeClient  *kubernetes.Clientset
	InformerFactory informers.SharedInformerFactory
	Framework       framework.Framework
}

func CreateDefaultConfig() *SchedulerConfig {
	hostConfig, err := rest.InClusterConfig()
	if err != nil {
		logger.Error("Failed to get cluster config", err)
		os.Exit(1)
	}
	hostKubeClient := kubernetes.NewForConfigOrDie(hostConfig)

	informerFactory := informers.NewSharedInformerFactory(hostKubeClient, 0)

	// Initialize plugins
	filterPlugins := []framework.FilterPlugin{
		plugin.NewNodeResourcesFit(),
	}

	scorePlugins := []framework.ScorePlugin{
		plugin.NewLeastAllocated(),
	}

	bindPlugin := plugin.NewDefaultBinder(hostKubeClient)

	// Create framework
	fwk := framework.NewFramework(filterPlugins, scorePlugins, bindPlugin)

	return &SchedulerConfig{
		InformerFactory: informerFactory,
		HostKubeClient:  hostKubeClient,
		Framework:       fwk,
	}
}
