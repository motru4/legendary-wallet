package kafkarepo

import (
	"context"
	"encoding/json"
	"fmt"
	"wallet-service/internal/models"

	"github.com/segmentio/kafka-go"
)

type OperationRepository struct {
	writer *kafka.Writer
}

func NewOperationRepository(writer *kafka.Writer) *OperationRepository {
	return &OperationRepository{
		writer: writer,
	}
}

// SendOperation sends operation to Kafka
func (r *OperationRepository) SendOperation(ctx context.Context, msg models.KafkaMessage) error {
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal kafka message: %w", err)
	}

	// Send message to Kafka
	// Use walletID as key to guarantee processing order for operations of the same wallet
	err = r.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(msg.WalletID),
		Value: msgBytes,
	})

	if err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

	return nil
}
