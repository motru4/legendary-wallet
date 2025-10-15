package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"wallet-service/internal/models"
	"wallet-service/internal/repositories/kafkarepo"
	"wallet-service/internal/repositories/postgresrepo"
	"wallet-service/internal/repositories/redisrepo"

	"github.com/google/uuid"
)

type WalletService struct {
	postgresRepo *postgresrepo.WalletRepository
	kafkaRepo    *kafkarepo.OperationRepository
	redisRepo    *redisrepo.WalletRepository
}

func NewWalletService(postgresRepo *postgresrepo.WalletRepository, redisRepo *redisrepo.WalletRepository, kafkaRepo *kafkarepo.OperationRepository) *WalletService {
	return &WalletService{
		postgresRepo: postgresRepo,
		kafkaRepo:    kafkaRepo,
		redisRepo:    redisRepo,
	}
}

func (s *WalletService) GetWalletBalance(ctx context.Context, walletID string) (*models.WalletBalanceResponse, error) {
	// Try to get balance from Redis cache first
	balance, err := s.redisRepo.GetBalance(ctx, walletID)
	if err == nil {
		return &models.WalletBalanceResponse{
			WalletID: walletID,
			Balance:  balance,
		}, nil
	}

	// If Redis error is not "balance not found", log it but continue to PostgreSQL
	if !errors.Is(err, redisrepo.ErrBalanceNotFound) {
		fmt.Printf("Redis cache error (non-critical): %v\n", err)
	}

	// Get wallet data from PostgreSQL
	wallet, err := s.postgresRepo.GetWallet(ctx, walletID)
	if err != nil {
		return nil, err
	}

	// Update Redis cache asynchronously with fresh data
	go func() {
		cacheCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := s.redisRepo.SetBalance(cacheCtx, walletID, wallet.Balance); err != nil {
			fmt.Printf("Failed to update redis cache for wallet %s: %v\n", walletID, err)
		}
	}()

	// Return balance from PostgreSQL
	return &models.WalletBalanceResponse{
		WalletID: walletID,
		Balance:  wallet.Balance,
	}, nil
}

func (s *WalletService) CreateWallet(ctx context.Context) (*models.WalletBalanceResponse, error) {
	walletID := uuid.New().String()

	// Create wallet in PostgreSQL
	if err := s.postgresRepo.CreateWallet(ctx, walletID); err != nil {
		return nil, fmt.Errorf("failed to create wallet: %w", err)
	}

	return &models.WalletBalanceResponse{
		WalletID: walletID,
		Balance:  0,
	}, nil
}

func (s *WalletService) GetOperation(ctx context.Context, walletID, operationID string) (*models.OperationStatusResponse, error) {
	// Check if wallet exists
	walletExists, err := s.postgresRepo.WalletExists(ctx, walletID)
	if err != nil {
		return nil, fmt.Errorf("failed to check wallet existence: %w", err)
	}
	if !walletExists {
		return nil, postgresrepo.ErrWalletNotFound
	}

	/// Get operation from PostgreSQL
	operation, err := s.postgresRepo.GetOperation(ctx, walletID, operationID)
	if err != nil {
		return nil, postgresrepo.ErrOperationNotFound
	}

	// Convert to response model
	response := &models.OperationStatusResponse{
		OperationID:   operation.ID,
		WalletID:      operation.WalletID,
		OperationType: operation.OperationType,
		Amount:        operation.Amount,
		Status:        operation.Status,
		ProcessedAt:   operation.ProcessedAt,
		Error:         operation.Error,
	}

	return response, nil
}

// CreateOperation creates an operation and sends it to Kafka
func (s *WalletService) CreateOperation(ctx context.Context, req models.WalletOperationRequest) (string, error) {
	// Check if wallet exists
	exists, err := s.postgresRepo.WalletExists(ctx, req.WalletID)
	if err != nil {
		return "", fmt.Errorf("failed to check wallet existence: %w", err)
	}
	if !exists {
		return "", postgresrepo.ErrWalletNotFound
	}

	// Create operation in PostgreSQL with PENDING status
	operationID, err := s.postgresRepo.CreateOperation(ctx, req.WalletID, req.OperationType, req.Amount)
	if err != nil {
		return "", fmt.Errorf("failed to create operation: %w", err)
	}

	// Send operation to Kafka for worker processing
	kafkaMsg := models.KafkaMessage{
		OperationID:   operationID,
		WalletID:      req.WalletID,
		OperationType: req.OperationType,
		Amount:        req.Amount,
	}

	if err := s.kafkaRepo.SendOperation(ctx, kafkaMsg); err != nil {
		// In case of Kafka error, mark operation as FAILED
		updateErr := s.postgresRepo.UpdateOperationStatus(ctx, operationID, "FAILED", fmt.Sprintf("Kafka error: %v", err))
		if updateErr != nil {
			// Log status update error, but return original Kafka error
			fmt.Printf("Failed to update operation status after Kafka error: %v\n", updateErr)
		}
		return "", fmt.Errorf("failed to send operation to queue: %w", err)
	}

	return operationID, nil
}
