package redisrepo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	expiration = 5 * time.Minute
)

var (
	ErrBalanceNotFound = errors.New("balance not found in cache")
)

type WalletRepository struct {
	client *redis.Client
	prefix string
}

func NewWalletRepository(client *redis.Client) *WalletRepository {
	return &WalletRepository{
		client: client,
		prefix: "wallet:",
	}
}

func (r *WalletRepository) SetBalance(ctx context.Context, walletID string, balance int64) error {
	key := r.getBalanceKey(walletID)

	balanceStr := strconv.FormatInt(balance, 10)

	err := r.client.Set(ctx, key, balanceStr, expiration).Err()
	if err != nil {
		return fmt.Errorf("failed to set balance in redis: %w", err)
	}

	return nil
}

func (r *WalletRepository) GetBalance(ctx context.Context, walletID string) (int64, error) {
	key := r.getBalanceKey(walletID)

	balanceStr, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, ErrBalanceNotFound
		}
		return 0, fmt.Errorf("failed to get balance from redis: %w", err)
	}

	balance, err := strconv.ParseInt(balanceStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse balance from redis: %w", err)
	}

	return balance, nil
}

func (r *WalletRepository) DeleteBalance(ctx context.Context, walletID string) error {
	key := r.getBalanceKey(walletID)

	err := r.client.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("failed to delete balance from redis: %w", err)
	}

	return nil
}

func (r *WalletRepository) getBalanceKey(walletID string) string {
	return r.prefix + walletID + ":balance"
}
