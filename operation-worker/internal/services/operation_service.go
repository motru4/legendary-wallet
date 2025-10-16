package services

import (
	"context"
	"fmt"
	"operation-worker/internal/models"
	"operation-worker/internal/repositories/postgresrepo"
	"operation-worker/internal/repositories/redisrepo"
	"time"
)

type WalletService struct {
	walletRepo *postgresrepo.WalletRepo
	cacheRepo  *redisrepo.WalletRepository
}

func NewWalletService(
	walletRepo *postgresrepo.WalletRepo,
	cacheRepo *redisrepo.WalletRepository,
) *WalletService {
	return &WalletService{
		walletRepo: walletRepo,
		cacheRepo:  cacheRepo,
	}
}

// ProcessWalletOperations обрабатывает батч операций для одного кошелька
func (s *WalletService) ProcessWalletOperations(walletID string, operations []models.KafkaMessage) error {
	ctx := context.Background()

	// Начинаем транзакцию
	txRepo, err := s.walletRepo.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Обрабатываем операции в транзакции
	processedBalance, operationsToUpdate, err := s.processOperationsInTx(ctx, txRepo, walletID, operations)
	if err != nil {
		if rollbackErr := txRepo.Rollback(); rollbackErr != nil {
			return fmt.Errorf("process error: %w, rollback error: %v", err, rollbackErr)
		}
		return fmt.Errorf("failed to process operations: %w", err)
	}

	// Массово обновляем статусы операций в БД
	if len(operationsToUpdate) > 0 {
		if err := txRepo.BulkUpdateOperations(ctx, operationsToUpdate); err != nil {
			if rollbackErr := txRepo.Rollback(); rollbackErr != nil {
				return fmt.Errorf("bulk update error: %w, rollback error: %v", err, rollbackErr)
			}
			return fmt.Errorf("failed to bulk update operations: %w", err)
		}
	}

	// Обновляем баланс кошелька
	if err := txRepo.UpdateBalance(ctx, walletID, processedBalance); err != nil {
		if rollbackErr := txRepo.Rollback(); rollbackErr != nil {
			return fmt.Errorf("update balance error: %w, rollback error: %v", err, rollbackErr)
		}
		return fmt.Errorf("failed to update wallet balance: %w", err)
	}

	// Коммитим транзакцию
	if err := txRepo.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Обновляем кэш (вне транзакции)
	if err := s.updateCache(ctx, walletID, processedBalance); err != nil {
		fmt.Printf("Warning: failed to update cache for wallet %s: %v\n", walletID, err)
	}

	return nil
}

// processOperationsInTx обрабатывает операции внутри транзакции и возвращает операции для обновления
func (s *WalletService) processOperationsInTx(
	ctx context.Context,
	txRepo *postgresrepo.TxWalletRepo,
	walletID string,
	operations []models.KafkaMessage,
) (int64, []models.WalletOperation, error) {

	// Блокируем кошелек для обновления
	wallet, err := txRepo.LockWalletForUpdate(ctx, walletID)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to lock wallet: %w", err)
	}

	currentBalance := wallet.Balance
	now := time.Now()
	operationsToUpdate := make([]models.WalletOperation, 0)

	// Получаем текущие операции из БД для проверки статусов
	operationIDs := make([]string, len(operations))
	for i, op := range operations {
		operationIDs[i] = op.OperationID
	}

	existingOperations, err := txRepo.GetOperationsByIDs(ctx, walletID, operationIDs)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to get operations: %w", err)
	}

	// Создаем мапу для быстрого доступа к существующим операциям
	existingOpsMap := make(map[string]models.WalletOperation)
	for _, op := range existingOperations {
		existingOpsMap[op.ID] = op
	}

	// Обрабатываем операции в порядке их поступления
	for _, operation := range operations {
		// Проверяем, существует ли операция и имеет ли статус PENDING
		existingOp, exists := existingOpsMap[operation.OperationID]
		if !exists {
			// Операция не найдена в БД - пропускаем
			fmt.Printf("Warning: operation %s not found in database\n", operation.OperationID)
			continue
		}

		// Проверяем статус операции - обрабатываем только PENDING
		if existingOp.Status != models.OperationStatusPending {
			// Операция уже обработана (PROCESSED или FAILED) - пропускаем
			continue
		}

		// Обрабатываем операцию и получаем обновленную версию
		newBalance, updatedOperation, err := s.processSingleOperation(
			operation, existingOp, currentBalance, now,
		)
		if err != nil {
			return 0, nil, fmt.Errorf("failed to process operation %s: %w", operation.OperationID, err)
		}

		// Добавляем операцию в список для массового обновления
		operationsToUpdate = append(operationsToUpdate, updatedOperation)

		// Обновляем баланс для следующих операций
		if updatedOperation.Status == models.OperationStatusProcessed {
			currentBalance = newBalance
		}
	}

	return currentBalance, operationsToUpdate, nil
}

// processSingleOperation обрабатывает одну операцию на основе существующей записи из БД
func (s *WalletService) processSingleOperation(
	operation models.KafkaMessage,
	existingOperation models.WalletOperation,
	currentBalance int64,
	now time.Time,
) (int64, models.WalletOperation, error) {

	// Используем существующую операцию как основу
	updatedOperation := existingOperation

	var newBalance int64
	var status string
	var errorMsg *string

	switch operation.OperationType {
	case models.OperationTypeDeposit:
		newBalance = currentBalance + operation.Amount
		status = models.OperationStatusProcessed
		processedAt := now
		updatedOperation.ProcessedAt = &processedAt

	case models.OperationTypeWithdraw:
		if currentBalance >= operation.Amount {
			newBalance = currentBalance - operation.Amount
			status = models.OperationStatusProcessed
			processedAt := now
			updatedOperation.ProcessedAt = &processedAt
		} else {
			status = models.OperationStatusFailed
			msg := "insufficient funds"
			errorMsg = &msg
			newBalance = currentBalance
		}

	default:
		status = models.OperationStatusFailed
		msg := fmt.Sprintf("unknown operation type: %s", operation.OperationType)
		errorMsg = &msg
		newBalance = currentBalance
	}

	updatedOperation.Status = status
	updatedOperation.Error = errorMsg

	return newBalance, updatedOperation, nil
}

func (s *WalletService) updateCache(ctx context.Context, walletID string, balance int64) error {
	if err := s.cacheRepo.SetBalance(ctx, walletID, balance); err != nil {
		return fmt.Errorf("failed to update cache: %w", err)
	}
	return nil
}
