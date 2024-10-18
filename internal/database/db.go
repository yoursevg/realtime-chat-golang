package database

import (
	"database/sql"
	"github.com/google/uuid"
	_ "github.com/lib/pq" // PostgreSQL driver
	"log"
	"time"
)

type Message struct {
	ID         int    `json:"id"`
	SenderID   int    `json:"sender_id"`
	ReceiverID int    `json:"receiver_id"`
	Content    string `json:"content"`
}

var db *sql.DB

// InitializeDatabase инициализирует подключение к базе данных
func InitializeDatabase(connStr string) error {
	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		return err
	}

	// Проверяем доступность базы данных с таймаутом
	for i := 0; i < 5; i++ { // Попытки подключения, например, 5 раз
		if err := db.Ping(); err == nil {
			break
		}
		log.Println("Waiting for database to be ready...")
		time.Sleep(2 * time.Second) // Ждем 2 секунды перед повторной попыткой
	}

	createTableQuery := `
	CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    message_id UUID UNIQUE,
    sender_id INT NOT NULL,
    receiver_id INT NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	`

	if _, err := db.Exec(createTableQuery); err != nil {
		return err
	}
	return nil
}

// SaveMessage saves a message to the database
func SaveMessage(messageID uuid.UUID, senderID, receiverID int, content string) error {
	query := `INSERT INTO messages (message_id, sender_id, receiver_id, content)
              VALUES ($1, $2, $3, $4)
              ON CONFLICT (message_id) DO NOTHING`
	_, err := db.Exec(query, messageID, senderID, receiverID, content)
	if err != nil {
		log.Printf("Failed to save message: %v", err)
	} else {
		log.Printf("Message saved successfully with message_id: %s", messageID)
	}
	return err
}

// CloseDatabase closes the database connection
func CloseDatabase() {
	if db != nil {
		db.Close()
	}
}
