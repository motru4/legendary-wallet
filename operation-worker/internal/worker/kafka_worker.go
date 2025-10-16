package worker

import (
	"context"
	"encoding/json"
	"log"
	"operation-worker/internal/models"
	"time"

	"github.com/IBM/sarama"
)

func (m *PartitionManager) runWorker(ctx context.Context, partition int, partitionConsumer sarama.PartitionConsumer, batchProcessor *BatchProcessor) {
	ticker := time.NewTicker(m.cfg.Worker.ProcessingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Context canceled - terminating work
			log.Printf("Partition %d: Shutdown signal received", partition)
			batchProcessor.ProcessRemaining()
			return

		case msg := <-partitionConsumer.Messages():
			// New message from Kafka
			var kafkaMsg models.KafkaMessage
			if err := json.Unmarshal(msg.Value, &kafkaMsg); err != nil {
				log.Printf("Partition %d: Failed to unmarshal message: %v", partition, err)
				continue
			}
			batchProcessor.AddMessage(msg, kafkaMsg)

		case err := <-partitionConsumer.Errors():
			// Error from Kafka
			log.Printf("Partition %d: Kafka error: %v", partition, err)

		case <-ticker.C:
			// The timer has triggered - we process the batch
			batchProcessor.ProcessBatch()
		}
	}
}
