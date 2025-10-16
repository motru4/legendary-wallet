package broker

import (
	"wallet-service/internal/config"

	"github.com/segmentio/kafka-go"
)

func NewKafkaWriter(cfg config.KafkaConfig) (*kafka.Writer, error) {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},    // Use hash balancer to guarantee order
		RequiredAcks: kafka.RequireOne, // Wait for acknowledgement from leader
		Async:        false,            // Synchronous writing for reliability
		MaxAttempts:  10,
	}

	return writer, nil
}
