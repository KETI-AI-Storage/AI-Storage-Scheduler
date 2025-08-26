package main

import (
	"log"
	"sync"
)

func main() {
	quitChan := make(chan struct{})
	var wg sync.WaitGroup

	hostConfig, err := rest.InClusterConfig()
	if err != nil {
		log.Fatal(err)
	}

	hostKubeClient := kubernetes.NewForConfigOrDie(hostConfig)

	

}
