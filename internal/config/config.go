package config

import (
	"os"

	logger "keti/ai-storage-scheduler/internal/backend/log"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type SchedulerConfig struct {
	HostKubeClient  *kubernetes.Clientset
	InformerFactory informers.SharedInformerFactory
	// Other Scheduling Configs...
}

func CreateDefaultConfig() *SchedulerConfig {
	hostConfig, err := rest.InClusterConfig()
	if err != nil {
		logger.Error("Failed to get cluster config", err)
		os.Exit(1)
	}
	hostKubeClient := kubernetes.NewForConfigOrDie(hostConfig)

	informerFactory := informers.NewSharedInformerFactory(hostKubeClient, 0)

	return &SchedulerConfig{
		InformerFactory: informerFactory,
		HostKubeClient:  hostKubeClient,
	}
}

// Scheduler Config 파일 업데이트 혹은 읽는
