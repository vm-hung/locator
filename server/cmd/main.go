package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/hunkvm/locator/pkg/interrupt"
	"github.com/hunkvm/locator/server"
	"github.com/hunkvm/locator/server/config"
)

func main() {
	path := flag.String("config", "config.yml", "Path to config file")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	conf, err := config.Load(*path)
	var logger *zap.Logger

	if err != nil {
		logger, _ = newLogger(nil)
		defer func() { logger.Sync() }()
		logger.Fatal("failed to load config", zap.Error(err))
	}

	if l, err := newLogger(&conf.Logger); err != nil {
		logger.Fatal("failed to create logger", zap.Error(err))
	} else {
		defer func() { l.Sync() }()
		logger = l
	}

	server := server.NewLocatorServer(conf, logger)
	go func() {
		if err := server.Start(); err != nil {
			logger.Fatal("failed to start locator server", zap.Error(err))
		}
	}()

	interruptorCb := &interrupt.InterruptorCallback{
		OnShutdownSignal: func() {
			logger.Info("shutdown signal received, starting graceful shutdown...")
		},
		OnContextCancelled: func() {
			logger.Info("context cancelled, stopping shutdown listener...")
		},
		OnNoCleanupTasks: func() {
			logger.Info("no cleanup tasks registered")
		},
		OnFinalizeStart: func() {
			logger.Info("beginning cleanup of registered tasks...")
		},
		OnTaskPanic: func(panicValue any) {
			logger.Error("a cleanup task panicked", zap.Any("panic", panicValue))
		},
		OnTaskCompleted: func() {
			logger.Info("all cleanup tasks completed successfully")
		},
		OnCleanupTimeout: func() {
			logger.Error("cleanup tasks exceeded timeout, forcing shutdown")
		},
	}

	interruptor := interrupt.New(
		interrupt.WithTimeout(30*time.Second),
		interrupt.WithCallback(interruptorCb),
	)

	interruptor.Register(func(ctx context.Context) {
		logger.Info("stopping locator server...")
		server.Stop()
	})
	interruptor.Listen()
}
