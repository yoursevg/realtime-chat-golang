package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	kafkago "github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	db "realtime-chat/internal/database"
	"realtime-chat/internal/kafka"
	"realtime-chat/internal/models"
	red "realtime-chat/internal/redis"
	"realtime-chat/internal/requests"
	"realtime-chat/internal/websockets"
	"time"
)

var (
	kafkaProducer *kafka.Producer
	ctx           = context.Background()
)

func main() {

	//Инициализация
	red.InitRedis()
	defer red.CloseRedis()

	db.InitializeDB()
	defer db.CloseDatabase()

	kafkaProducer = kafka.NewKafkaProducer("chat-topic")

	// Запускаем Kafka Consumer в отдельной горутине
	go startConsumer()

	// Настраиваем маршруты HTTP
	r := mux.NewRouter()
	r.HandleFunc("/ws", websockets.HandleWebSocket)
	r.HandleFunc("/send-message", SendMessage).Methods("POST")
	r.HandleFunc("/messages", GetMessages).Methods("GET")

	// Запускаем сервер
	log.Println("Server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}

// SendMessage Handler для отправки сообщения через Kafka Producer
func SendMessage(w http.ResponseWriter, r *http.Request) {
	var messageReq requests.MessageRequest
	if err := json.NewDecoder(r.Body).Decode(&messageReq); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Генерируем уникальный ID для сообщения
	messageID := uuid.New()

	// Формируем сообщение для сохранения
	message := models.Message{
		MessageID:  messageID,
		SenderID:   messageReq.SenderID,
		ReceiverID: messageReq.ReceiverID,
		Content:    messageReq.Content,
		CreatedAt:  time.Now().Format(time.RFC3339),
	}

	// Сохраняем сообщение в PostgreSQL
	if err := db.SaveMessage(message); err != nil {
		http.Error(w, "Failed to save message to database", http.StatusInternalServerError)
		log.Error(err)
	}

	// Сохраняем сообщение в Redis
	if err := red.CacheMessage(message); err != nil {
		http.Error(w, "Failed to cache message", http.StatusInternalServerError)
		log.Error(err)
	}

	// Отправляем сообщение в Kafka
	messageJSON, _ := json.Marshal(message)
	err := kafkaProducer.SendMessage("chat-key", string(messageJSON))
	if err != nil {
		http.Error(w, "Failed to send message", http.StatusInternalServerError)
		log.Error(err)
	}

	_, err = w.Write([]byte("Message sent to Kafka, saved in DB and cached in Redis"))
	if err != nil {
		log.Error(err)
	}
}

// GetMessages Получение сообщений из редиса
func GetMessages(w http.ResponseWriter, r *http.Request) {
	messages, err := red.GetMessages(ctx)
	if err != nil {
		http.Error(w, "Failed to get messages from Redis", http.StatusInternalServerError)
		log.Error(err)
	}

	// Устанавливаем заголовок контента
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(messages); err != nil {
		http.Error(w, "Failed to encode messages", http.StatusInternalServerError)
		log.Error(err)
	}
}

// processMessage processes a Kafka message
func processMessage(message kafkago.Message) error {

	var messageReq requests.MessageRequest
	if err := json.Unmarshal(message.Value, &messageReq); err != nil {
		log.Printf("Error unmarshalling message: %v", err)
		return err
	}

	return nil
}

// Kafka Consumer для чтения сообщений
func startConsumer() {

	topic := "chat-topic"
	brokerAddress := os.Getenv("KAFKA_BROKER")

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        []string{brokerAddress},
		GroupID:        "chat-group",
		Topic:          topic,
		Partition:      0,
		MinBytes:       10e3,        // 10KB
		MaxBytes:       10e6,        // 10MB
		CommitInterval: time.Second, // Автоматически сохранять смещения каждую секунду
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
