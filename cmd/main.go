package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	kafkago "github.com/segmentio/kafka-go"
	"log"
	"net/http"
	"os"
	"realtime-chat/internal/database"
	"realtime-chat/internal/kafka"
	"time"
)

var (
	kafkaProducer *kafka.Producer
	redisClient   *redis.Client // Redis клиент
	ctx           = context.Background()
)

func main() {
	dbConnStr := "postgres://" + os.Getenv("DB_USER") + ":" + os.Getenv("DB_PASSWORD") +
		"@postgres-db:5432/chat_service?sslmode=disable&connect_timeout=5" // добавлено sslmode=disable для отключения SSL

	log.Printf("Connecting to database with user: %s", os.Getenv("DB_USER"))
	log.Println("Database connection string:", dbConnStr)

	err := database.InitializeDatabase(dbConnStr)
	if err != nil {
		log.Fatalf("Could not connect to the database: %v", err)
	}

	defer database.CloseDatabase()

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}
	redisClient = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// Проверка подключения к Redis
	_, err = redisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}

	// Получаем адрес брокера Kafka из переменных окружения
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "kafka:9092"
	}
	kafkaProducer = kafka.NewKafkaProducer(kafkaBroker, "chat-topic")

	// Запускаем Kafka Consumer в отдельной горутине
	go startConsumer()

	// Настраиваем маршруты HTTP
	r := mux.NewRouter()
	r.HandleFunc("/send-message", SendMessage).Methods("POST")
	r.HandleFunc("/messages", GetMessages).Methods("GET")

	// Запускаем сервер
	log.Println("Server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}

type MessageRequest struct {
	SenderID   int    `json:"sender_id"`
	ReceiverID int    `json:"receiver_id"`
	Content    string `json:"content"`
}

// SendMessage Handler для отправки сообщения через Kafka Producer
func SendMessage(w http.ResponseWriter, r *http.Request) {
	var messageReq MessageRequest
	if err := json.NewDecoder(r.Body).Decode(&messageReq); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Добавляем сообщение в Redis с ограниченным временем хранения
	messageJSON, _ := json.Marshal(messageReq)
	err := redisClient.Set(ctx, fmt.Sprintf("message-%d", time.Now().UnixNano()), messageJSON, 10*time.Minute).Err()
	if err != nil {
		http.Error(w, "Failed to cache message", http.StatusInternalServerError)
		return
	}

	// Отправляем сообщение в Kafka
	err = kafkaProducer.SendMessage("chat-key", string(messageJSON))
	if err != nil {
		http.Error(w, "Failed to send message", http.StatusInternalServerError)
		return
	}

	w.Write([]byte("Message sent to Kafka and cached in Redis"))
}

// GetMessages Новый Handler для получения сообщений
func GetMessages(w http.ResponseWriter, r *http.Request) {
	keys, err := redisClient.Keys(ctx, "message-*").Result()
	if err != nil {
		http.Error(w, "Failed to get messages from cache", http.StatusInternalServerError)
		return
	}

	var messages []MessageRequest
	for _, key := range keys {
		val, err := redisClient.Get(ctx, key).Result()
		if err != nil {
			continue
		}

		var message MessageRequest
		if err := json.Unmarshal([]byte(val), &message); err == nil {
			messages = append(messages, message)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(messages); err != nil {
		http.Error(w, "Failed to encode messages", http.StatusInternalServerError)
		return
	}
}

// processMessage processes a Kafka message and saves it to the database
func processMessage(message kafkago.Message) error {

	// Шаг 1: Распаковываем сообщение
	var messageReq MessageRequest
	if err := json.Unmarshal(message.Value, &messageReq); err != nil {
		log.Printf("Error unmarshalling message: %v", err)
		return err
	}

	// Шаг 2: Генерируем уникальный UUID для сообщения
	messageID, err := uuid.NewUUID() // Используем Google UUID для генерации уникального идентификатора
	if err != nil {
		log.Printf("Failed to generate message UUID: %v", err)
		return err
	}

	// Шаг 3: Сохраняем сообщение в базу данных с уникальным message_id
	if err := database.SaveMessage(messageID, messageReq.SenderID, messageReq.ReceiverID, messageReq.Content); err != nil {
		log.Printf("Failed to save message: %v", err)
		return err
	}

	log.Printf("Message from sender %d to receiver %d saved successfully", messageReq.SenderID, messageReq.ReceiverID)
	return nil
}

// Kafka Consumer для чтения сообщений
func startConsumer() {

	topic := "chat-topic"
	brokerAddress := os.Getenv("KAFKA_BROKER")

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   []string{brokerAddress},
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	defer reader.Close()

	fmt.Println("Listening for messages from Kafka...")

	for {
		message, err := reader.FetchMessage(context.Background())
		if err != nil {
			log.Fatal("Error while fetching message:", err)
		}

		// Обрабатываем сообщение через функцию processMessage
		if err := processMessage(message); err == nil {
			// Коммитим смещение сообщения только если оно было успешно обработано
			if err := reader.CommitMessages(context.Background(), message); err != nil {
				log.Printf("Failed to commit offset: %v", err)
			} else {
				fmt.Printf("Successfully processed and committed message: %s\n", string(message.Value))
			}
		} else {
			log.Printf("Error processing message: %v", err)
		}
	}
}
