package websockets

import (
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	"net/http"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// Разрешаем запросы с любого источника
		return true
	},
}

// HandleWebSocket - обрабатывает новые WebSocket-соединения
func HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {

		log.WithError(err).Error("Failed to upgrade connection")
		return
	}
	defer conn.Close()

	// Обрабатываем сообщения от клиента
	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			log.WithError(err).Error("ReadMessage error:")
			break
		}
		log.Printf("Received message: %s", string(message))

		// Отправляем обратно клиенту
		if err = conn.WriteMessage(messageType, message); err != nil {
			log.Println("WriteMessage error:", err)
			break
		}
	}
}
