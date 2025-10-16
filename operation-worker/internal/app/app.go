package app

import (
	"context"
	"fmt"
	"log"
	"operation-worker/internal/cache"
	"operation-worker/internal/config"
	"operation-worker/internal/database"
	"operation-worker/internal/repositories/postgresrepo"
	"operation-worker/internal/repositories/redisrepo"
	"operation-worker/internal/services"
	"operation-worker/internal/worker"
	"os"
	"os/signal"
	"syscall"
)

type App struct {
	cfg              *config.Config
	walletService    *services.WalletService
	partitionManager *worker.PartitionManager
}

func New() (*App, error) {
	a := new(App)

	// Initialize config
	a.cfg = config.New()

	// Connect to database
	db, err := database.NewPostgres(a.cfg.Postgres.URL)
	if err != nil {
		return nil, fmt.Errorf("database connection error: %w", err)
	}

	// Connect to cache
	redis, err := cache.NewRedis(a.cfg.Redis)
	if err != nil {
		return nil, fmt.Errorf("cache connection error: %w", err)
	}

	// Initialize repositories
	postgresRepo := postgresrepo.NewdWalletRepo(db)
	redisRepo := redisrepo.NewWalletRepository(redis)

	// Initialize services
	a.walletService = services.NewWalletService(postgresRepo, redisRepo)

	// Partition Manager
	a.partitionManager = worker.NewPartitionManager(a.cfg, a.walletService)

	return a, nil
}

func (a *App) Run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	a.partitionManager.Start(ctx)
}
