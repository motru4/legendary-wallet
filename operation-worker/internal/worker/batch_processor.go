package worker

import (
	"log"
	"operation-worker/internal/models"
	"operation-worker/internal/services"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type BatchProcessor struct {
	partitionID   int
	walletService *services.WalletService
	messages      []*sarama.ConsumerMessage
	kafkaMessages []models.KafkaMessage
	mutex         sync.Mutex
	lastProcessed time.Time
}

func NewBatchProcessor(partitionID int, walletService *services.WalletService) *BatchProcessor {
	return &BatchProcessor{
		partitionID:   partitionID,
		walletService: walletService,
		messages:      make([]*sarama.ConsumerMessage, 0),
		kafkaMessages: make([]models.KafkaMessage, 0),
		lastProcessed: time.Now(),
	}
}

func (bp *BatchProcessor) AddMessage(msg *sarama.ConsumerMessage, kafkaMsg models.KafkaMessage) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	bp.messages = append(bp.messages, msg)
	bp.kafkaMessages = append(bp.kafkaMessages, kafkaMsg)
}

func (bp *BatchProcessor) ProcessBatch() {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	if len(bp.messages) == 0 {
		return
	}

	log.Printf("Partition %d: Processing batch of %d messages", bp.partitionID, len(bp.messages))

	walletOperations := bp.groupByWallet()

	// Process transactions for each wallet
	for walletID, operations := range walletOperations {
		if err := bp.walletService.ProcessWalletOperations(walletID, operations); err != nil {
			log.Printf("Partition %d: Failed to process operations for wallet %s: %v",
				bp.partitionID, walletID, err)
			// Сontinue processing other wallets
			continue
		}
	}

	// Clear the batch
	bp.messages = bp.messages[:0]
	bp.kafkaMessages = bp.kafkaMessages[:0]
	bp.lastProcessed = time.Now()

	log.Printf("Partition %d: Batch processed successfully", bp.partitionID)
}

func (bp *BatchProcessor) ProcessRemaining() {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	if len(bp.messages) > 0 {
		log.Printf("Partition %d: Processing remaining %d messages before shutdown",
			bp.partitionID, len(bp.messages))
		bp.ProcessBatch()
	}
}

func (bp *BatchProcessor) groupByWallet() map[string][]models.KafkaMessage {
	walletOperations := make(map[string][]models.KafkaMessage)

	for _, msg := range bp.kafkaMessages {
		walletOperations[msg.WalletID] = append(walletOperations[msg.WalletID], msg)
	}

	return walletOperations
}
