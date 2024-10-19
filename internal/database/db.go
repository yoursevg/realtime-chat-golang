package database

import (
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	_ "github.com/lib/pq" // PostgreSQL driver
	log "github.com/sirupsen/logrus"
	"os"
)

type Message struct {
	ID         int    `json:"id"`
	SenderID   int    `json:"sender_id"`
	ReceiverID int    `json:"receiver_id"`
	Content    string `json:"content"`
}

var db *sql.DB

// InitializeDB инициализирует подключение к базе данных
func InitializeDB() {
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbHost := os.Getenv("DB_HOST")
	dbName := os.Getenv("DB_NAME")

	dbConnStr := fmt.Sprintf("postgres://%s:%s@%s:5432/%s?sslmode=disable&connect_timeout=5",
		dbUser, dbPassword, dbHost, dbName)

	log.Printf("Connecting to database with user: %s", dbUser)
	log.Println("Database connection string:", dbConnStr)

	var err error
	db, err = sql.Open("postgres", dbConnStr)
	if err != nil {
		log.WithError(err).Fatal("Error connecting to database")
	}

	if err := db.Ping(); err != nil {
		log.WithError(err).Fatal("Error pinging database")
	}

	log.Println("Connected to the database successfully.")

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
		log.WithError(err).Fatal("Error creating init table")
	}
}

// SaveMessage saves a message to the database
func SaveMessage(messageID uuid.UUID, senderID, receiverID int, content string) error {
	query := `INSERT INTO messages (message_id, sender_id, receiver_id, content)
              VALUES ($1, $2, $3, $4)
              ON CONFLICT (message_id) DO NOTHING`
	_, err := db.Exec(query, messageID, senderID, receiverID, content)
	if err != nil {
		log.WithError(err).Error("Failed to save message to the database")
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
