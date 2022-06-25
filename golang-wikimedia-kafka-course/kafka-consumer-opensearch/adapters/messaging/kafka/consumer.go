package kafka

import (
	"context"

	"github.com/Shopify/sarama"
)

type KafkaConsumer struct {
	topic   string
	group   sarama.ConsumerGroup
	handler sarama.ConsumerGroupHandler
}

func NewConsumerGroup(topic string, groupId string, handler sarama.ConsumerGroupHandler) *KafkaConsumer {
	config := sarama.NewConfig()
	// config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = false
	group, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, groupId, config)
	if err != nil {
		panic(err)
	}

	return &KafkaConsumer{
		topic:   topic,
		group:   group,
		handler: handler,
	}
}

func (kc *KafkaConsumer) ConsumeMessage(ctx context.Context) error {
	return kc.group.Consume(ctx, []string{kc.topic}, kc.handler)
}

func (kc *KafkaConsumer) Close() {
	kc.group.Close()
}
