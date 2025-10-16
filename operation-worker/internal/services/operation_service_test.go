package services

import (
	"testing"
	"time"

	"operation-worker/internal/models"
	"operation-worker/internal/repositories/postgresrepo"
	"operation-worker/internal/repositories/redisrepo"
)

func strptr(s string) *string { return &s }

func TestWalletService_processSingleOperation(t *testing.T) {
	now := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)

	type want struct {
		newBalance       int64
		status           string
		processedAtSet   bool
		errorMsg         *string
		preserveBaseData bool
	}

	tests := []struct {
		name              string
		walletRepo        *postgresrepo.WalletRepo
		cacheRepo         *redisrepo.WalletRepository
		operation         models.KafkaMessage
		existingOperation models.WalletOperation
		currentBalance    int64
		want              want
	}{
		{
			name: "deposit: increases balance, marks processed, sets ProcessedAt",
			operation: models.KafkaMessage{
				OperationID:   "op-1",
				WalletID:      "w-1",
				OperationType: models.OperationTypeDeposit,
				Amount:        150,
			},
			existingOperation: models.WalletOperation{
				ID:            "op-1",
				WalletID:      "w-1",
				OperationType: models.OperationTypeDeposit,
				Amount:        150,
				Status:        models.OperationStatusPending,
				CreatedAt:     now.Add(-time.Minute),
				ProcessedAt:   nil,
				Error:         nil,
			},
			currentBalance: 1000,
			want: want{
				newBalance:       1150,
				status:           models.OperationStatusProcessed,
				processedAtSet:   true,
				errorMsg:         nil,
				preserveBaseData: true,
			},
		},
		{
			name: "withdraw: decreases balance, marks processed, sets ProcessedAt",
			operation: models.KafkaMessage{
				OperationID:   "op-2",
				WalletID:      "w-1",
				OperationType: models.OperationTypeWithdraw,
				Amount:        200,
			},
			existingOperation: models.WalletOperation{
				ID:            "op-2",
				WalletID:      "w-1",
				OperationType: models.OperationTypeWithdraw,
				Amount:        200,
				Status:        models.OperationStatusPending,
				CreatedAt:     now.Add(-time.Minute),
			},
			currentBalance: 1000,
			want: want{
				newBalance:       800,
				status:           models.OperationStatusProcessed,
				processedAtSet:   true,
				errorMsg:         nil,
				preserveBaseData: true,
			},
		},
		{
			name: "withdraw: insufficient funds -> failed, keeps balance, no ProcessedAt, sets error",
			operation: models.KafkaMessage{
				OperationID:   "op-3",
				WalletID:      "w-2",
				OperationType: models.OperationTypeWithdraw,
				Amount:        2000,
			},
			existingOperation: models.WalletOperation{
				ID:            "op-3",
				WalletID:      "w-2",
				OperationType: models.OperationTypeWithdraw,
				Amount:        2000,
				Status:        models.OperationStatusPending,
				CreatedAt:     now.Add(-time.Minute),
			},
			currentBalance: 1000,
			want: want{
				newBalance:       1000,
				status:           models.OperationStatusFailed,
				processedAtSet:   false,
				errorMsg:         strptr("insufficient funds"),
				preserveBaseData: true,
			},
		},
		{
			name: "unknown operation type -> failed, keeps balance, no ProcessedAt, sets error",
			operation: models.KafkaMessage{
				OperationID:   "op-4",
				WalletID:      "w-3",
				OperationType: "BONUS",
				Amount:        500,
			},
			existingOperation: models.WalletOperation{
				ID:            "op-4",
				WalletID:      "w-3",
				OperationType: "BONUS",
				Amount:        500,
				Status:        models.OperationStatusPending,
				CreatedAt:     now.Add(-time.Minute),
			},
			currentBalance: 3000,
			want: want{
				newBalance:       3000,
				status:           models.OperationStatusFailed,
				processedAtSet:   false,
				errorMsg:         strptr("unknown operation type: BONUS"),
				preserveBaseData: true,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			s := NewWalletService(tt.walletRepo, tt.cacheRepo)

			newBal, updated, err := s.processSingleOperation(tt.operation, tt.existingOperation, tt.currentBalance, now)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// 1) Проверяем баланс.
			if newBal != tt.want.newBalance {
				t.Fatalf("new balance: got %d, want %d", newBal, tt.want.newBalance)
			}

			// 2) Проверяем статус.
			if updated.Status != tt.want.status {
				t.Fatalf("status: got %q, want %q", updated.Status, tt.want.status)
			}

			// 3) Проверяем ProcessedAt (должен/не должен быть установлен).
			if tt.want.processedAtSet {
				if updated.ProcessedAt == nil {
					t.Fatalf("ProcessedAt: got nil, want set to now")
				}
				// для точности — равен ли он exactly now
				if !updated.ProcessedAt.Equal(now) {
					t.Fatalf("ProcessedAt: got %v, want %v", updated.ProcessedAt, now)
				}
			} else {
				if updated.ProcessedAt != nil {
					t.Fatalf("ProcessedAt: got %v, want nil", updated.ProcessedAt)
				}
			}

			// 4) Проверяем Error.
			switch {
			case tt.want.errorMsg == nil && updated.Error != nil:
				t.Fatalf("Error: got %v, want nil", *updated.Error)
			case tt.want.errorMsg != nil && updated.Error == nil:
				t.Fatalf("Error: got nil, want %v", *tt.want.errorMsg)
			case tt.want.errorMsg != nil && updated.Error != nil && *updated.Error != *tt.want.errorMsg:
				t.Fatalf("Error: got %q, want %q", *updated.Error, *tt.want.errorMsg)
			}

			// 5) База из existingOperation должна сохраниться.
			if tt.want.preserveBaseData {
				if updated.ID != tt.existingOperation.ID {
					t.Fatalf("preserve ID: got %q, want %q", updated.ID, tt.existingOperation.ID)
				}
				if updated.WalletID != tt.existingOperation.WalletID {
					t.Fatalf("preserve WalletID: got %q, want %q", updated.WalletID, tt.existingOperation.WalletID)
				}
				if updated.Amount != tt.existingOperation.Amount {
					t.Fatalf("preserve Amount: got %d, want %d", updated.Amount, tt.existingOperation.Amount)
				}
				if updated.OperationType != tt.existingOperation.OperationType {
					t.Fatalf("preserve OperationType: got %q, want %q", updated.OperationType, tt.existingOperation.OperationType)
				}
				if !updated.CreatedAt.Equal(tt.existingOperation.CreatedAt) {
					t.Fatalf("preserve CreatedAt: got %v, want %v", updated.CreatedAt, tt.existingOperation.CreatedAt)
				}
			}
		})
	}
}
