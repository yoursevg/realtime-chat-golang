package websockets

import (
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	"net/http"
	"realtime-chat/internal/kafka"
)

// Конфигурация апгрейдера для WebSocket
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// HandleWebSocket Обработчик для новых WebSocket-соединений
func HandleWebSocket(hub *Hub, kafka *kafka.Producer, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Error upgrading connection:", err)
		return
	}

	// Логирование подключения нового клиента
	log.Println("New WebSocket connection established")

	client := &Client{
		Hub:      hub,
		Conn:     conn,
		Send:     make(chan []byte, 256),
		Producer: kafka,
	}
	hub.Register <- client

	// Логирование регистрации нового клиента
	log.Println("Client registered")

	// Запуск чтения и записи для клиента в отдельных горутинах
	go func() {
		client.Read()
		// Логирование отключения клиента при завершении чтения
		log.Println("WebSocket connection closed")
		hub.Unregister <- client
	}()
	go client.Write()
}
