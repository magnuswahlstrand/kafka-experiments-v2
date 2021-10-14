package main

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"log"
)

func main() {
	url := "localhost:9092"
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{url}, config)
	if err != nil {
		log.Fatal(err)
	}

	topic := "test"
	event := map[string]interface{}{
		"age":  34,
		"name": "magnus",
	}

	b, err := json.Marshal(event)
	if err != nil {
		log.Fatal(err)
	}

	kafkaMessage := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
	}

	_, _, err = producer.SendMessage(kafkaMessage)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("successfully published!")
}
