package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

type Config struct {
	Postgres PostgresConfig
	Kafka    KafkaConfig
	Redis    RedisConfig
	Worker   WorkerConfig
}

type PostgresConfig struct {
	URL string
}

type KafkaConfig struct {
	Brokers    []string
	Topic      string
	Partitions int
	// Sarama-specific
	Version       string
	ConsumerGroup string
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	PoolSize int
}

type WorkerConfig struct {
	ProcessingInterval time.Duration
}

func New() *Config {
	return &Config{
		Postgres: PostgresConfig{
			URL: os.Getenv("POSTGRES_URL"),
		},
		Kafka: KafkaConfig{
			Brokers: strings.Split((os.Getenv("KAFKA_BROKERS")), ","),
			Topic:   os.Getenv("KAFKA_TOPIC"),
			Partitions: func(pt string) int {
				kafkaPartitions, _ := strconv.Atoi(pt)
				return kafkaPartitions
			}(os.Getenv("KAFKA_PARTITIONS")),
			Version:       os.Getenv("KAFKA_VERSION"),
			ConsumerGroup: os.Getenv("KAFKA_CONSUMER_GROUP"),
		},
		Redis: RedisConfig{
			Addr:     os.Getenv("REDIS_ADDR"),
			Password: os.Getenv("REDIS_PASSWORD"),
			DB: func(db string) int {
				redisDB, _ := strconv.Atoi(db)
				return redisDB
			}(os.Getenv("REDIS_DB")),
			PoolSize: func(ps string) int {
				redisPoolSize, _ := strconv.Atoi(ps)
				return redisPoolSize
			}(os.Getenv("REDIS_POOL_SIZE")),
		},
		Worker: WorkerConfig{
			ProcessingInterval: func(pi string) time.Duration {
				processingInterval, _ := strconv.Atoi(pi)
				return time.Duration(processingInterval) * time.Second
			}(os.Getenv("WORKER_PROCESSING_INTERVAL")),
		},
	}
}

func (k *KafkaConfig) GetSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()

	if k.Version != "" {
		version, err := sarama.ParseKafkaVersion(k.Version)
		if err == nil {
			config.Version = version
		}
	}

	// Consumer settings
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 2 * time.Minute
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// Settings for batch processing
	config.Consumer.Fetch.Min = 1
	config.Consumer.Fetch.Default = 1024 * 1024 // 1MB
	config.Consumer.MaxWaitTime = 100 * time.Millisecond

	// Network настройки
	config.Net.MaxOpenRequests = 5
	config.Net.DialTimeout = 30 * time.Second
	config.Net.ReadTimeout = 30 * time.Second
	config.Net.WriteTimeout = 30 * time.Second

	return config
}
