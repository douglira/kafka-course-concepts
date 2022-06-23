package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"

	"github.com/douglira/kafka-producer-wikimedia/adapters/messaging"
	"github.com/douglira/kafka-producer-wikimedia/adapters/sse"
	"github.com/douglira/kafka-producer-wikimedia/business/eventsourcing"
)

const (
	WIKIMEDIA_URI = "https://stream.wikimedia.org/v2/stream/recentchange"
)

func main() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	sseClient := sse.NewClient(WIKIMEDIA_URI)
	wikimediaEventSource := eventsourcing.NewWikimedia(sseClient)
	wikimediaEventSource.HandleMessage()

	topic := "wikimedia.recentchange"
	kafkaProducer := messaging.NewProducer(topic)

	for {
		select {
		case message := <-wikimediaEventSource.Successes():
			me := sse.MessageEvent{}

			json.Unmarshal(message, &me)
			dataByte, err := json.Marshal(&me.Data)

			if err != nil {
				log.Println("Parse event source payload error", err)
				continue
			}

			kafkaProducer.SendMessage("", dataByte)
		case err := <-wikimediaEventSource.Errors():
			log.Println("Unexpected error", err)

		case <-signals:
			kafkaProducer.AsyncClose()
		}
	}
}
