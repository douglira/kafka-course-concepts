package main

import (
	"context"
	"log"

	"github.com/douglira/kafka-consumer-opensearch/adapters/db"
	"github.com/douglira/kafka-consumer-opensearch/adapters/messaging/kafka"
)

const (
	WIKIMEDIA_OPENSEARCH_INDEX = "wikimedia"
	KAFKA_TOPIC                = "wikimedia.recentchange"
	KAFKA_GROUP_ID             = "consumer-opensearch"
)

func main() {
	ctx := context.Background()

	opensearchClient := db.NewElasticSearchClient(WIKIMEDIA_OPENSEARCH_INDEX)
	opensearchClient.CreateIndex()
	wikimediaStorage := db.NewWikimedia(opensearchClient)

	wikimediaConsumerHandler := kafka.NewWikimediaConsumer(wikimediaStorage)
	wikimediaKafkaConsumer := kafka.NewConsumerGroup(KAFKA_TOPIC, KAFKA_GROUP_ID, wikimediaConsumerHandler)
	defer wikimediaKafkaConsumer.Close()

	for {
		err := wikimediaKafkaConsumer.ConsumeMessage(ctx)
		if err != nil {
			log.Println("Error at kafka consumer group", err)
		}
	}
}
