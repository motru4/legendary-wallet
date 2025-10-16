package postgresrepo

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type WalletRepo struct {
	db *sqlx.DB
}

func NewdWalletRepo(db *sqlx.DB) *WalletRepo {
	return &WalletRepo{db: db}
}

// BeginTx starts a transaction and returns a transactional repository
func (r *WalletRepo) BeginTx(ctx context.Context) (*TxWalletRepo, error) {
	tx, err := r.db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	return NewTxWalletRepo(tx), nil
}
