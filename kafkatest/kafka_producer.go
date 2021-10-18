package kafkatest

import (
	"encoding/json"
	"github.com/Shopify/sarama"
)

type TopicSyncProducer struct {
	sarama.SyncProducer
	topic string
}

func (p *TopicSyncProducer) SendJSONMessage(event interface{}) error {
	b, err := json.Marshal(event)
	if err != nil {
		return err
	}

	kafkaMessage := &sarama.ProducerMessage{
		Topic: p.topic,
		Value: sarama.ByteEncoder(b),
	}

	_, _, err = p.SyncProducer.SendMessage(kafkaMessage)
	if err != nil {
		return err
	}
	return nil
}

func NewSyncProducer(addr []string, topic string) (*TopicSyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(addr, config)
	if err != nil {
		return nil, err
	}
	return &TopicSyncProducer{
		SyncProducer: producer,
		topic:        topic,
	}, nil
}
