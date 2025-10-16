package postgresrepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"operation-worker/internal/models"
	"strings"

	"github.com/jmoiron/sqlx"
)

type TxWalletRepo struct {
	tx *sqlx.Tx
}

func NewTxWalletRepo(tx *sqlx.Tx) *TxWalletRepo {
	return &TxWalletRepo{tx: tx}
}

func (r *TxWalletRepo) Commit() error {
	return r.tx.Commit()
}

func (r *TxWalletRepo) Rollback() error {
	return r.tx.Rollback()
}

func (r *TxWalletRepo) LockWalletForUpdate(ctx context.Context, walletID string) (*models.Wallet, error) {
	var wallet models.Wallet
	query := `SELECT id, balance FROM wallets WHERE id = $1 FOR UPDATE`
	err := r.tx.GetContext(ctx, &wallet, query, walletID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("wallet not found: %s", walletID)
		}
		return nil, fmt.Errorf("failed to lock wallet: %w", err)
	}
	return &wallet, nil
}

func (r *TxWalletRepo) UpdateBalance(ctx context.Context, walletID string, balance int64) error {
	query := `UPDATE wallets SET balance = $1, updated_at = NOW() WHERE id = $2`
	result, err := r.tx.ExecContext(ctx, query, balance, walletID)
	if err != nil {
		return fmt.Errorf("failed to update balance: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("wallet not found: %s", walletID)
	}

	return nil
}

func (r *TxWalletRepo) GetOperationsByIDs(ctx context.Context, walletID string, operationIDs []string) ([]models.WalletOperation, error) {
	if len(operationIDs) == 0 {
		return []models.WalletOperation{}, nil
	}

	query, args, err := sqlx.In(`
		SELECT id, wallet_id, operation_type, amount, status, created_at, processed_at, error
		FROM wallet_operations 
		WHERE wallet_id = ? AND id IN (?)
		ORDER BY created_at ASC
	`, walletID, operationIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	query = r.tx.Rebind(query)
	var operations []models.WalletOperation
	err = r.tx.SelectContext(ctx, &operations, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get operations: %w", err)
	}

	return operations, nil
}

func (r *TxWalletRepo) BulkUpdateOperations(ctx context.Context, operations []models.WalletOperation) error {
	if len(operations) == 0 {
		return nil
	}

	// Prepare a batch request
	batchSize := 100 // Batch size to prevent too large requests
	for i := 0; i < len(operations); i += batchSize {
		end := i + batchSize
		if end > len(operations) {
			end = len(operations)
		}

		batch := operations[i:end]
		if err := r.updateBatch(ctx, batch); err != nil {
			return fmt.Errorf("failed to update batch [%d:%d]: %w", i, end, err)
		}
	}

	return nil
}

func (r *TxWalletRepo) updateBatch(ctx context.Context, ops []models.WalletOperation) error {
	if len(ops) == 0 {
		return nil
	}

	args := make([]interface{}, 0, 5*len(ops))
	values := make([]string, 0, len(ops))

	for i, op := range ops {
		base := i*5 + 1
		values = append(values,
			fmt.Sprintf("($%d::uuid,$%d::uuid,$%d::text,$%d::timestamptz,$%d::text)",
				base, base+1, base+2, base+3, base+4,
			),
		)

		args = append(args,
			op.ID,
			op.WalletID,
			op.Status,
			op.ProcessedAt,
			op.Error,
		)
	}

	query := fmt.Sprintf(`
		UPDATE wallet_operations AS w
		SET
			status = v.status,
			processed_at = v.processed_at,
			error = v.error
		FROM (VALUES
			%s
		) AS v(id, wallet_id, status, processed_at, error)
		WHERE w.id = v.id AND w.wallet_id = v.wallet_id
	`, strings.Join(values, ","))

	if _, err := r.tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("bulk UPDATE FROM VALUES failed: %w", err)
	}
	return nil
}
