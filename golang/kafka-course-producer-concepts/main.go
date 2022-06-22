package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"

	sarama "github.com/Shopify/sarama"
)

func NewProducer() sarama.AsyncProducer {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	// config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		panic(err)
	}

	return producer
}

func SendMessage(producer sarama.AsyncProducer, topic string, key string, message interface{}) {
	messageKey := sarama.StringEncoder("")
	if key != "" {
		messageKey = sarama.StringEncoder(key)
	}
	jsonValue, _ := json.Marshal(message)

	msg := &sarama.ProducerMessage{
		Key:   messageKey,
		Topic: topic,
		Value: sarama.ByteEncoder(jsonValue),
	}

	producer.Input() <- msg
}

func main() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	p1 := NewProducer()
	user := map[string]interface{}{
		"name":     "Douglas Lira",
		"birthday": "1995-07-18",
		"career":   "Software Engineer",
	}

	for i := 0; i < 10; i++ {
		min := 1000
		max := 100000000000
		user["random"] = rand.Intn(max-min) + min
		key := fmt.Sprint("key_", i)
		SendMessage(p1, "user-update", key, user)
	}

ProducerLoop:
	for {
		select {
		case successes := <-p1.Successes():
			log.Println(
				"Message sent successfully\n",
				"Topic:", successes.Topic, "\n",
				"Key:", successes.Key, "\n",
				"Partition:", successes.Partition, "\n",
				"Offset:", successes.Offset, "\n",
				"Timestamp:", successes.Timestamp,
			)
		case errors := <-p1.Errors():
			log.Println("An error occurred", errors)
		case <-signals:
			log.Println("Closing producer")
			p1.AsyncClose()
			break ProducerLoop
		}
	}
}
