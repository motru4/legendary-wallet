package app

import (
	"fmt"
	"net/http"
	"time"
	_ "wallet-service/docs"
	"wallet-service/internal/broker"
	"wallet-service/internal/cache"
	"wallet-service/internal/config"
	"wallet-service/internal/database"
	"wallet-service/internal/repositories/kafkarepo"
	"wallet-service/internal/repositories/postgresrepo"
	"wallet-service/internal/repositories/redisrepo"
	"wallet-service/internal/services"
	"wallet-service/internal/transport/http/handler"
)

type App struct {
	cfg        *config.Config
	httpServer *http.Server
}

// @title Wallet API
// @version 1.0
// @description This is a sample server for a wallet service.
// @host localhost:8080
// @BasePath /api/v1
// @schemes http
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

	// Connect to broker
	kafka, err := broker.NewKafkaWriter(a.cfg.Kafka)
	if err != nil {
		return nil, fmt.Errorf("broker connection error: %w", err)
	}

	// Initialize repositories
	postgresRepo := postgresrepo.NewWalletRepository(db)
	redisRepo := redisrepo.NewWalletRepository(redis)
	kafkaRepo := kafkarepo.NewOperationRepository(kafka)

	// Initialize services
	walletService := services.NewWalletService(postgresRepo, redisRepo, kafkaRepo)

	// Initialize mux and handlers
	mux := http.NewServeMux()

	handler.NewWallet(mux, walletService)

	// Initialize http server
	a.httpServer = &http.Server{
		Addr:         a.cfg.Server.Port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	return a, nil
}

func (a *App) Run() error {
	fmt.Printf("Starting HTTP server on port %s\n", a.cfg.Server.Port)
	if err := a.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http server error: %w", err)
	}

	return nil
}
