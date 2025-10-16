package broker

import (
	"operation-worker/internal/config"
	"time"

	"github.com/segmentio/kafka-go"
)

func NewKafkaPartitionReader(cfg *config.KafkaConfig, partition int) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:   cfg.Brokers,
		Topic:     cfg.Topic,
		Partition: partition,
		MinBytes:  10,
		MaxBytes:  10e6,
		MaxWait:   100 * time.Millisecond,
	})
}
