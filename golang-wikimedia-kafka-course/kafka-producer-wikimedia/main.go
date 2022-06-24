package main

import (
	"encoding/json"
	"log"
	"math"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	"github.com/douglira/kafka-producer-wikimedia/adapters/messaging/kafka"
	"github.com/douglira/kafka-producer-wikimedia/adapters/sse"
	"github.com/douglira/kafka-producer-wikimedia/business/eventsourcing"
)

const (
	WIKIMEDIA_URI = "https://stream.wikimedia.org/v2/stream/recentchange"
	TOPIC         = "wikimedia.recentchange"
)

var logger = log.New(os.Stderr, "", log.LstdFlags)

func main() {
	sarama.Logger = logger
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	sseClient := sse.NewClient(WIKIMEDIA_URI)
	wikimediaEventSource := eventsourcing.NewWikimedia(sseClient)
	sseClient.Listen(wikimediaEventSource)

	c := sarama.NewConfig()
	// Set safe producer config
	c.Net.MaxOpenRequests = 1
	c.Producer.Retry.Max = 5
	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Producer.Idempotent = true
	c.Producer.Retry.Max = math.MaxInt32
	// Faster to transfer data and less latency
	c.Producer.Compression = sarama.CompressionSnappy
	c.Producer.Flush.Frequency = 20
	c.Producer.Flush.Bytes = 32 << (10 * 1)
	kafkaProducer := kafka.NewProducer(TOPIC, c)

	for {
		select {
		case message := <-wikimediaEventSource.Successes():
			me := sse.MessageEvent{}

			json.Unmarshal(message, &me)

			kafkaProducer.SendMessage("", []byte(me.Data))
		case err := <-wikimediaEventSource.Errors():
			logger.Println("Unexpected error", err)

		case <-signals:
			kafkaProducer.AsyncClose()
		}
	}
}
