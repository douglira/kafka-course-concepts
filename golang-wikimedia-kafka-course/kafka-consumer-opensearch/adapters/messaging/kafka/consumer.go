package kafka

import (
	"context"

	"github.com/Shopify/sarama"
)

type KafkaConsumer struct {
	topic string
	group sarama.ConsumerGroup
}

func NewConsumerGroup(topic string, groupId string) *KafkaConsumer {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	g, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, groupId, config)
	if err != nil {
		panic(err)
	}

	return &KafkaConsumer{
		topic: topic,
		group: g,
	}
}

func (kc *KafkaConsumer) ConsumeMessage(ctx context.Context, handler sarama.ConsumerGroupHandler) error {
	return kc.group.Consume(ctx, []string{kc.topic}, handler)
}

func (kc *KafkaConsumer) Close() {
	kc.group.Close()
}
