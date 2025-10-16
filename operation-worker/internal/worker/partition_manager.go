package worker

import (
	"context"
	"fmt"
	"log"
	"operation-worker/internal/config"
	"operation-worker/internal/services"
	"sync"

	"github.com/IBM/sarama"
)

type PartitionManager struct {
	cfg           *config.Config
	walletService *services.WalletService
	wg            sync.WaitGroup
}

func NewPartitionManager(cfg *config.Config, operationService *services.WalletService) *PartitionManager {
	return &PartitionManager{
		cfg:           cfg,
		walletService: operationService,
	}
}

func (m *PartitionManager) Start(ctx context.Context) error {
	log.Printf("Starting workers for %d partitions", m.cfg.Kafka.Partitions)

	consumer, err := sarama.NewConsumer(m.cfg.Kafka.Brokers, m.cfg.Kafka.GetSaramaConfig())
	if err != nil {
		return fmt.Errorf("failed to create Kafka consumer: %w", err)
	}
	defer consumer.Close()

	for partition := 0; partition < m.cfg.Kafka.Partitions; partition++ {
		m.wg.Add(1)
		go m.startWorkerForPartition(ctx, consumer, partition)
	}

	// Wait for all workers to complete to prevent program termination
	m.wg.Wait()
	log.Println("All partition workers stopped")
	return nil
}

func (m *PartitionManager) startWorkerForPartition(ctx context.Context, consumer sarama.Consumer, partition int) {
	defer m.wg.Done()

	log.Printf("Starting worker for partition %d", partition)

	// Create a PartitionConsumer for a specific partition
	partitionConsumer, err := consumer.ConsumePartition(
		m.cfg.Kafka.Topic,
		int32(partition),
		sarama.OffsetNewest,
	)
	if err != nil {
		log.Printf("Partition %d: Failed to create partition consumer: %v", partition, err)
		return
	}
	defer partitionConsumer.Close()

	// Create a BatchProcessor for this partition
	batchProcessor := NewBatchProcessor(partition, m.walletService)

	// Start the main worker loop
	m.runWorker(ctx, partition, partitionConsumer, batchProcessor)
}
