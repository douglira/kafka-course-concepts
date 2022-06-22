package main

import (
	"context"
	"log"

	sarama "github.com/Shopify/sarama"
)

type UserUpdateConsumerGroupHandler struct{}

func (UserUpdateConsumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {

	log.Println(sess.Claims())
	// Reseting offset of consumers within consumer group and fetching all messages again
	// partitions, exists := sess.Claims()["user-update"]
	// if exists {
	// 	for p := range partitions {
	// 		sess.ResetOffset("user-update", int32(p), 0, "")
	// 	}
	// }
	return nil
}
func (UserUpdateConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (UserUpdateConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Println(
			"Message sent successfully\n",
			"Topic:", message.Topic, "\n",
			"Key:", string(message.Key), "\n",
			"Partition:", message.Partition, "\n",
			"Offset:", message.Offset, "\n",
			"Timestamp:", message.Timestamp, "\n",
			"Valeu:", string(message.Value),
		)
		sess.MarkMessage(message, "")
	}
	return nil
}

func NewConsumerGroup(topic string) sarama.ConsumerGroup {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	g, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, topic, config)
	if err != nil {
		panic(err)
	}

	return g
}

func main() {
	g := NewConsumerGroup("user-update-group")
	defer func() { _ = g.Close() }()

	ctx := context.Background()
	for {
		topics := []string{"user-update"}
		handler := UserUpdateConsumerGroupHandler{}

		err := g.Consume(ctx, topics, handler)
		if err != nil {
			panic(err)
		}
	}
}
