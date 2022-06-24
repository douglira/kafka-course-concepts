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

	kafkaConsumer := kafka.NewConsumerGroup(KAFKA_TOPIC, KAFKA_GROUP_ID)
	defer kafkaConsumer.Close()

	wikimediaStorage := db.NewWikimedia(opensearchClient, ctx)
	wikimediaConsumerHandler := kafka.WikimediaConsumer{Storage: wikimediaStorage}

	for {
		err := kafkaConsumer.ConsumeMessage(ctx, wikimediaConsumerHandler)
		if err != nil {
			log.Println("Error at kafka consumer group", err)
		}
	}
}
