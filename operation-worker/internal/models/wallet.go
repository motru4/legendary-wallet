package models

import "time"

// Database model
type Wallet struct {
	ID        string    `db:"id"`
	Balance   int64     `db:"balance"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

type WalletOperation struct {
	ID            string     `db:"id"`
	WalletID      string     `db:"wallet_id"`
	OperationType string     `db:"operation_type"`
	Amount        int64      `db:"amount"`
	Status        string     `db:"status"` // PENDING, PROCESSED, FAILED
	CreatedAt     time.Time  `db:"created_at"`
	ProcessedAt   *time.Time `db:"processed_at"`
	Error         *string    `db:"error"`
}

type KafkaMessage struct {
	OperationID   string `json:"operation_id"`
	WalletID      string `json:"wallet_id"`
	OperationType string `json:"operation_type"`
	Amount        int64  `json:"amount"`
}

// Status constants
const (
	OperationStatusPending   = "PENDING"
	OperationStatusProcessed = "PROCESSED"
	OperationStatusFailed    = "FAILED"
)

// Message constants
const (
	MessageOperationQueued = "Operation queued for processing"
	MessageWalletCreated   = "Wallet successfully created"
)

// Operation type constants
const (
	OperationTypeDeposit  = "DEPOSIT"
	OperationTypeWithdraw = "WITHDRAW"
)
