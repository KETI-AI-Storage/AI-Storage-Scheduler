package config

import (
	"os"

	corev1 "k8s.io/api/core/v1"

	logger "keti/ai-storage-scheduler/internal/log"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type SchedulerConfig struct {
	HostKubeClient           *kubernetes.Clientset
	InformerFactory          informers.SharedInformerFactory
	DynInformerFactory       dynamicinformer.DynamicSharedInformerFactory
	percentageOfNodesToScore int32
}

func CreateDefaultConfig() *SchedulerConfig {
	hostConfig, err := rest.InClusterConfig()
	if err != nil {
		logger.Error("Failed to get cluster config", err)
		os.Exit(1)
	}
	hostKubeClient := kubernetes.NewForConfigOrDie(hostConfig)

	informerFactory := informers.NewSharedInformerFactory(hostKubeClient, 0)

	dynClient := dynamic.NewForConfigOrDie(hostConfig)
	dynInformerFactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dynClient, 0, corev1.NamespaceAll, nil)

	return &SchedulerConfig{
		InformerFactory:          informerFactory,
		HostKubeClient:           hostKubeClient,
		DynInformerFactory:       dynInformerFactory,
		percentageOfNodesToScore: 0,
	}
}

// Scheduler Config 파일 업데이트 혹은 읽는
