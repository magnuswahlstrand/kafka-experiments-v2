package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/magnuswahlstrand/kafkalib"
	"log"
	"strings"
)

type Event struct {
	ID   uuid.UUID `json:"id"`
	Type string    `json:"type"`
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

func messageReceived(payload []byte) error {
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
	addrList := flag.String("addr", "localhost:9092", "")
	topic := flag.String("topic", "test", "")
	flag.Parse()
	addr := strings.Split(*addrList, ",")

	consumerGroupID := "some-consumer-group"

	handler := kafkalib.NewHandler(
		messageReceived,
		kafkalib.HandlerOptions{
			Setup:   func(_ sarama.ConsumerGroupSession) error { fmt.Println("setup complete"); return nil },
			Cleanup: func(_ sarama.ConsumerGroupSession) error { fmt.Println("clean up"); return nil },
		})
	consumer, err := kafkalib.NewConsumer(addr, []string{*topic}, consumerGroupID, handler)
	if err != nil {
		log.Fatal(err)
	}

	if err := consumer.Start(context.Background()); err != nil {
		log.Fatal(err)
	}
}
