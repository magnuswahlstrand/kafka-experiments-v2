package main

import (
	"encoding/json"
	"flag"
	"github.com/Shopify/sarama"
	"log"
	"strings"
)

func main() {
	addrList := flag.String("addr","localhost:9092","")
	topic := flag.String("topic","test","")
	flag.Parse()
	addr := strings.Split(*addrList, ",")

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(addr, config)
	if err != nil {
		log.Fatal(err)
	}

	event := map[string]interface{}{
		"age":  34,
		"name": "magnus",
	}

	b, err := json.Marshal(event)
	if err != nil {
		log.Fatal(err)
	}

	kafkaMessage := &sarama.ProducerMessage{
		Topic: *topic,
		Value: sarama.ByteEncoder(b),
	}

	_, _, err = producer.SendMessage(kafkaMessage)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("successfully published!")
}
