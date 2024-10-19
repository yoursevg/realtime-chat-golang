package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
)

type Producer struct {
	writer *kafka.Writer
}

func NewKafkaProducer(topic string) *Producer {
	// Получаем адрес брокера Kafka из переменных окружения
	brokerAddress := os.Getenv("KAFKA_BROKER")
	if brokerAddress == "" {
		brokerAddress = "kafka:9092"
	}
	return &Producer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokerAddress),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (p *Producer) SendMessage(key string, message string) error {
	err := p.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(key),
			Value: []byte(message),
		},
	)
	if err != nil {
		log.Printf("Failed to write message: %v", err)
		return err
	}
	log.Printf("Message sent to Kafka: %s", message)
	return nil
}

func (p *Producer) Close() {
	p.writer.Close()
}
