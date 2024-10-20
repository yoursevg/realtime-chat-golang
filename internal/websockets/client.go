package websockets

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	db "realtime-chat/internal/database"
	kafka "realtime-chat/internal/kafka"
	"realtime-chat/internal/models"
	red "realtime-chat/internal/redis"
	"realtime-chat/internal/requests"
	"time"
)

// Client Тип для хранения данных о клиенте WebSocket
type Client struct {
	Hub      *Hub            // Ссылка на Hub
	Conn     *websocket.Conn // WebSocket-соединение
	Send     chan []byte     // Канал для отправки сообщений клиенту
	Producer *kafka.Producer // Продюсер кафки
}

func (c *Client) Read() {
	defer func() {
		c.Hub.Unregister <- c
		c.Conn.Close()
	}()

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			log.Printf("Error reading message from WebSocket: %v", err)
			break
		}

		// Декодируем сообщение из JSON
		var messageReq requests.MessageRequest
		if err := json.Unmarshal(message, &messageReq); err != nil {
			log.Printf("Error unmarshalling message: %v", err)
			log.Println(message)
			continue
		}

		// Генерируем уникальный ID для сообщения
		messageID := uuid.New()

		// Формируем сообщение для сохранения
		chatMessage := models.Message{
			MessageID:  messageID,
			SenderID:   messageReq.SenderID,
			ReceiverID: messageReq.ReceiverID,
			Content:    messageReq.Content,
			CreatedAt:  time.Now().Format(time.RFC3339),
		}

		// Сохраняем сообщение в базе данных
		if err := db.SaveMessage(chatMessage); err != nil {
			log.Printf("Error saving message to database: %v", err)
			continue
		}

		// Отправляем сообщение в Kafka
		messageJSON, _ := json.Marshal(chatMessage)
		if err := c.Producer.SendMessage("chat-key", string(messageJSON)); err != nil {
			log.Printf("Error sending message to Kafka: %v", err)
			continue
		}

		// Сохраняем сообщение в Redis
		if err := red.CacheMessage(chatMessage); err != nil {
			log.Printf("Error caching message in Redis: %v", err)
			continue
		}

		// Отправляем сообщение в хаб для широковещательной рассылки
		c.Hub.Broadcast <- message
	}
}

// WritePump Метод для записи сообщений клиенту
func (c *Client) Write() {
	defer c.Conn.Close()
	for {
		select {
		case message, ok := <-c.Send:
			if !ok {
				// Если канал закрыт, закрываем соединение
				err := c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				if err != nil {
					log.WithError(err).Error("Channel is closed")
					return
				}
				return
			}
			err := c.Conn.WriteMessage(websocket.TextMessage, message)
			if err != nil {
				log.WithError(err).Error("Error writing message")
				return
			}
		}
	}
}
