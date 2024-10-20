package redis

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"os"
	"realtime-chat/internal/models"
	"time"
)

var ctx = context.Background()
var rdb *redis.Client

// InitRedis инициализирует подключение к Redis
func InitRedis() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}
	rdb = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	// Проверка подключения к Redis
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.WithError(err).Error("Could not connect to Redis")
	}
}

// CacheMessage сохраняет сообщение в Redis
func CacheMessage(message models.Message) error {
	messageID := message.MessageID.String()
	msgBytes, err := json.Marshal(message)
	if err != nil {
		log.WithError(err).Error("Failed to marshal message for Redis")
		return err
	}

	// Сохраняем сообщение в Redis с таймаутом
	err = rdb.Set(ctx, "message:"+messageID, msgBytes, 10*time.Minute).Err()
	if err != nil {
		log.WithError(err).Error("Failed to cache message in Redis")
		return err
	}

	log.WithField("message_id", messageID).Info("Message cached successfully")
	return nil
}

// GetMessages получает все сообщения из Redis
func GetMessages(ctx context.Context) ([]models.Message, error) {
	keys, err := rdb.Keys(ctx, "message:*").Result()
	if err != nil {
		log.Printf("Failed to get message keys from Redis: %v", err)
		return nil, err
	}

	var messages []models.Message
	for _, key := range keys {
		val, err := rdb.Get(ctx, key).Result()
		if err != nil {
			log.Printf("Failed to get message from Redis for key %s: %v", key, err)
			continue // Пропускаем сообщение, если произошла ошибка
		}

		var message models.Message
		if err := json.Unmarshal([]byte(val), &message); err != nil {
			log.Printf("Failed to unmarshal message from Redis for key %s: %v", key, err)
			continue // Пропускаем сообщение, если оно невалидно
		}

		messages = append(messages, message)
	}

	return messages, nil
}

// CloseRedis закрывает подключение к Redis
func CloseRedis() {
	err := rdb.Close()
	if err != nil {
		log.WithError(err).Error("Failed to close Redis connection")
	} else {
		log.Info("Redis connection closed successfully")
	}
}
