package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
)

type Consumer struct {
}

func (*Consumer) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (*Consumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (c *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Println("received claim")
		fmt.Printf("received claim: topic %q partition %d\n", msg.Topic, msg.Partition)

		var s map[string]interface{}

		err := json.Unmarshal(msg.Value, &s)
		if err != nil {
			log.Println("ERR", err)
			continue
		}

		fmt.Println("success!")
		fmt.Println(s)
		sess.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	url := "localhost:9092"

	topics := []string{"test"}
	consumerGroup := "some-consumer-group"
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Consumer.Return.Errors = true

	client, err := sarama.NewClient([]string{url}, config)
	if err != nil {
		log.Fatal(err)
	}

	group, err := sarama.NewConsumerGroupFromClient(consumerGroup, client)
	if err != nil {
		_ = client.Close()
		log.Fatal(err)
	}

	// Handle errors
	go func() {
		for err := range group.Errors() {
			fmt.Println("Some error", err)
		}
	}()

	var consumer Consumer

	err = group.Consume(context.Background(), topics, &consumer)
	if err != nil {
		log.Fatal(err)
	}
}
