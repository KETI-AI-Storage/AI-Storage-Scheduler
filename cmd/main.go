package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	logger "keti/ai-storage-scheduler/internal/backend/log"
	config "keti/ai-storage-scheduler/internal/config"
	scheduler "keti/ai-storage-scheduler/internal/scheduler"
)

func main() {
	var err error

	// 0. initialize logger
	logger.Init(logger.NewDefaultConfig())

	// 1. generate config
	config := config.CreateDefaultConfig()

	// 2. initialize scheduler
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduler.MainScheduler, err = scheduler.NewScheduler(ctx, config)
	if err != nil {
		logger.Error("Failed to generate main sheduler", err)
		os.Exit(1)
	}
	err = scheduler.MainScheduler.InitScheduler()
	if err != nil {
		logger.Error("Failed to initialize main scheduler", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("Starting main scheduler...")
		scheduler.MainScheduler.Run(ctx)
		logger.Info("Main scheduler stopped")
	}()

	// 4. wait for waitgroup
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signalChan)

	sig := <-signalChan
	logger.Info("Received signal: %v, shutting down...", sig)

	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("All components stopped gracefully")
	case <-time.After(30 * time.Second):
		logger.Warn("Timeout waiting for components to stop")
	}

	logger.Info("Custom scheduler shutdown complete")
	os.Exit(0)
}
