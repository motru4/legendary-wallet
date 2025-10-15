package postgresrepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"wallet-service/internal/models"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

var (
	ErrWalletNotFound    = errors.New("wallet not found")
	ErrOperationNotFound = errors.New("operation not found")
)

type WalletRepository struct {
	db *sqlx.DB
}

func NewWalletRepository(db *sqlx.DB) *WalletRepository {
	return &WalletRepository{db: db}
}

// GetWallet get a wallet by ID
func (r *WalletRepository) GetWallet(ctx context.Context, walletID string) (*models.Wallet, error) {
	var wallet models.Wallet

	query := `SELECT id, balance, created_at, updated_at FROM wallets WHERE id = $1`

	err := r.db.QueryRowContext(ctx, query, walletID).Scan(
		&wallet.ID,
		&wallet.Balance,
		&wallet.CreatedAt,
		&wallet.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrWalletNotFound
		}
		return nil, fmt.Errorf("failed to get wallet from postgres: %w", err)
	}

	return &wallet, nil
}

// CreateWallet create a new wallet
func (r *WalletRepository) CreateWallet(ctx context.Context, walletID string) error {
	query := `INSERT INTO wallets (id, balance) VALUES ($1, $2)`

	_, err := r.db.ExecContext(ctx, query, walletID, 0)
	if err != nil {
		return fmt.Errorf("failed to create wallet: %w", err)
	}

	return nil
}

// GetOperation get the operation by wallet ID and operation ID
func (r *WalletRepository) GetOperation(ctx context.Context, walletID, operationID string) (*models.WalletOperation, error) {
	var operation models.WalletOperation

	query := `
		SELECT 
			id, wallet_id, operation_type, amount, status, 
			created_at, processed_at, error
		FROM wallet_operations 
		WHERE wallet_id = $1 AND id = $2
	`

	err := r.db.QueryRowContext(ctx, query, walletID, operationID).Scan(
		&operation.ID,
		&operation.WalletID,
		&operation.OperationType,
		&operation.Amount,
		&operation.Status,
		&operation.CreatedAt,
		&operation.ProcessedAt,
		&operation.Error,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrOperationNotFound
		}
		return nil, fmt.Errorf("failed to get operation from postgres: %w", err)
	}

	return &operation, nil
}

// WalletExists check the existence of a wallet
func (r *WalletRepository) WalletExists(ctx context.Context, walletID string) (bool, error) {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM wallets WHERE id = $1)`

	err := r.db.QueryRowContext(ctx, query, walletID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check wallet existence: %w", err)
	}

	return exists, nil
}

// CreateOperation create a new operation with the status PENDING
func (r *WalletRepository) CreateOperation(ctx context.Context, walletID, operationType string, amount int64) (string, error) {
	operationID := uuid.New().String()

	query := `
		INSERT INTO wallet_operations 
		(id, wallet_id, operation_type, amount, status, created_at)
		VALUES ($1, $2, $3, $4, 'PENDING', NOW())
	`

	_, err := r.db.ExecContext(ctx, query, operationID, walletID, operationType, amount)
	if err != nil {
		return "", fmt.Errorf("failed to create operation: %w", err)
	}

	return operationID, nil
}

// UpdateOperationStatus update the operation status
func (r *WalletRepository) UpdateOperationStatus(ctx context.Context, operationID, status, errorMsg string) error {
	query := `
		UPDATE wallet_operations 
		SET status = $1, processed_at = NOW(), error = $2
		WHERE id = $3
	`

	result, err := r.db.ExecContext(ctx, query, status, errorMsg, operationID)
	if err != nil {
		return fmt.Errorf("failed to update operation status: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("operation not found")
	}

	return nil
}
