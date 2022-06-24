package kafka

import (
	"encoding/json"
	// "fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/douglira/kafka-consumer-opensearch/adapters/db"
	"github.com/douglira/kafka-consumer-opensearch/business/models"
)

type WikimediaConsumer struct {
	Storage db.Storage
}

func extractMessageId(messageValue []byte) string {
	msg := models.Wikimedia{}

	json.Unmarshal(messageValue, &msg)

	return msg.Meta.Id
}

func (WikimediaConsumer) Setup(sess sarama.ConsumerGroupSession) error { return nil }
func (WikimediaConsumer) Cleanup(_ sarama.ConsumerGroupSession) error  { return nil }
func (w WikimediaConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// Idempotent Consumer - Strategy 1
		// Define an ID using Kafka Record coordinates if data does not have an ID
		// id := fmt.Sprint(message.Topic, "_", message.Partition, "_", message.Offset)

		// Idempotent Consumer - Strategy 2
		// Get the ID from JSON value
		id := extractMessageId(message.Value)

		data := string(message.Value)
		err := w.Storage.Upsert(data, id)
		if err != nil {
			log.Println("[WikimediaConsumer] Error during processing message", err)
			return err
		}

		sess.MarkMessage(message, "")
	}
	return nil
}
