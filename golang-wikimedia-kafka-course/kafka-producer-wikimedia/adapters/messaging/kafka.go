package messaging

import (
	"github.com/Shopify/sarama"
)

type KafkaProducer struct {
	topic    string
	producer sarama.AsyncProducer
}

func NewProducer(topic string, config *sarama.Config) KafkaProducer {
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		panic(err)
	}

	kp := KafkaProducer{
		topic,
		producer,
	}

	return kp
}

func (kp *KafkaProducer) SendMessage(key string, message []byte) {
	messageKey := sarama.StringEncoder(key)

	msg := &sarama.ProducerMessage{
		Key:   messageKey,
		Topic: kp.topic,
		Value: sarama.ByteEncoder(message),
	}

	kp.producer.Input() <- msg
}

func (kp *KafkaProducer) AsyncClose() {
	kp.producer.AsyncClose()
}

func (kp *KafkaProducer) Close() {
	kp.producer.AsyncClose()
}
