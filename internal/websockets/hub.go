package websockets

import "sync"

// Hub Тип для хранения всех подключенных клиентов
type Hub struct {
	Clients    map[*Client]bool // Карта активных клиентов
	Broadcast  chan []byte      // Канал для широковещательных сообщений
	Register   chan *Client     // Канал для регистрации нового клиента
	Unregister chan *Client     // Канал для отключения клиента
	mu         sync.Mutex       // Для потокобезопасной работы с картой клиентов
}

// Run Метод для запуска хаба (центрального узла чата)
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register:
			// Новый клиент подключился
			h.mu.Lock()
			h.Clients[client] = true
			h.mu.Unlock()
		case client := <-h.Unregister:
			// Клиент отключился
			h.mu.Lock()
			if _, ok := h.Clients[client]; ok {
				delete(h.Clients, client)
				close(client.Send)
			}
			h.mu.Unlock()
		case message := <-h.Broadcast:
			// Широковещательная рассылка сообщения всем клиентам
			h.mu.Lock()
			for client := range h.Clients {
				select {
				case client.Send <- message:
				default:
					// Если не удалось отправить сообщение, закрываем соединение
					close(client.Send)
					delete(h.Clients, client)
				}
			}
			h.mu.Unlock()
		}
	}
}
