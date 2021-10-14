package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/magnuswahlstrand/kafka-experiments-v2/library"
	"log"
)

type EventHandler struct{}

func (*EventHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (*EventHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h *EventHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Println("received claim")
		fmt.Printf("received claim: topic %q partition %d\n", msg.Topic, msg.Partition)
		if err := h.messageReceived(msg.Value); err != nil {
			return err
		}

		sess.MarkMessage(msg, "")
	}
	return nil
}

type Event struct {
	ID uuid.UUID `json:"id"`
	Type string `json:"type"`
}

func (e Event) Validate() error {
	if e.ID == uuid.Nil {
		return errors.New("event ID is empty")
	}
	if e.Type != "my-event-type" {
		return fmt.Errorf("invalid event type %s", e.Type)
	}
	return nil
}

func (h *EventHandler) messageReceived(payload []byte) error {
	var event Event
	if err := json.Unmarshal(payload, &event); err != nil {
		return err
	}

	if err := event.Validate(); err != nil {
		return err
	}

	// Do something here
	fmt.Println("do something here")

	return nil
}

func main() {
	url := "localhost:9092"
	topics := []string{"test"}
	consumerGroupID := "some-consumer-group"
	handler := &EventHandler{}
	consumer, err := library.NewConsumer(url, topics, consumerGroupID, handler)
	if err != nil {
		log.Fatal(err)
	}

	if err := consumer.Start(context.Background()); err != nil {
		log.Fatal(err)
	}
}
