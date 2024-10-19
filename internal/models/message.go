package models

import "github.com/google/uuid"

type Message struct {
	MessageID  uuid.UUID `json:"message_id"`
	SenderID   int       `json:"sender_id"`
	ReceiverID int       `json:"receiver_id"`
	Content    string    `json:"content"`
	CreatedAt  string    `json:"created_at"`
}
