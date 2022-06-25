package kafka

import (
	"encoding/json"
	// "fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/douglira/kafka-consumer-opensearch/adapters/db"
	"github.com/douglira/kafka-consumer-opensearch/business/models"
)

type wikimediaConsumer struct {
	Storage db.Storage
}

func NewWikimediaConsumer(storage db.Storage) wikimediaConsumer {
	return wikimediaConsumer{Storage: storage}
}

func extractMessageId(messageValue []byte) string {
	msg := models.Wikimedia{}

	json.Unmarshal(messageValue, &msg)

	return msg.Meta.Id
}

func (wikimediaConsumer) Setup(sess sarama.ConsumerGroupSession) error { return nil }
func (w wikimediaConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := sess.Context()
	payloadMap := map[string]string{}
	for message := range claim.Messages() {
		// Idempotent Consumer - Strategy 1
		// Define an ID using Kafka Record coordinates if data does not have an ID
		// id := fmt.Sprint(message.Topic, "_", message.Partition, "_", message.Offset)

		// Idempotent Consumer - Strategy 2
		// Get the ID from JSON value
		id := extractMessageId(message.Value)
		data := string(message.Value)
		p, err := db.AppendBulkInsert(payloadMap, data, id)
		if err != nil {
			log.Println("[WikimediaConsumer] Error during append bulk upsert processing", err, message.Topic, message.Partition, message.Offset)
		}
		payloadMap = p

		if payloadMap["count"] == "500" {
			bulkData := payloadMap["payload"]
			err := w.Storage.BulkUpsert(ctx, bulkData)
			if err != nil {
				log.Println("[WikimediaConsumer] Error during bulk upsert processing", err, message.Topic, message.Partition, message.Offset, data)
			}

			payloadMap = map[string]string{}
			sess.MarkMessage(message, "")
			sess.Commit()
		}

	}
	return nil
}

func (wikimediaConsumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
