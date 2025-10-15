package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Server   ServerConfig
	Postgres PostgresConfig
	Kafka    KafkaConfig
	Redis    RedisConfig
}

type ServerConfig struct {
	Port string
}

type PostgresConfig struct {
	URL string
}

type KafkaConfig struct {
	Brokers []string
	Topic   string
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	PoolSize int
}

func New() *Config {
	return &Config{
		Server: ServerConfig{
			Port: os.Getenv("SERVER_PORT"),
		},
		Postgres: PostgresConfig{
			URL: os.Getenv("POSTGRES_URL"),
		},
		Kafka: KafkaConfig{
			Brokers: strings.Split((os.Getenv("KAFKA_BROKERS")), ","),
			Topic:   os.Getenv("KAFKA_TOPIC"),
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
	}
}
