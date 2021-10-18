package kafka_experiments_v2

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
)


type Consumer struct {
	client  sarama.Client
	group   sarama.ConsumerGroup
	handler sarama.ConsumerGroupHandler
	topics  []string
}

func (c *Consumer) Start(ctx context.Context) error {
	if err := c.group.Consume(ctx, c.topics, c.handler); err != nil {
		return err
	}
	return nil
}

func (c *Consumer) Shutdown() error {
	if err := c.group.Close(); err != nil {
		return err
	}
	if err := c.client.Close(); err != nil {
		return err
	}
	return nil
}

func NewConsumer(addr []string, topics []string, consumerGroupID string, handler sarama.ConsumerGroupHandler) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V1_1_1_0
	config.Consumer.Return.Errors = true

	client, err := sarama.NewClient(addr, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	group, err := sarama.NewConsumerGroupFromClient(consumerGroupID, client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed to create kafka consumer group: %w", err)
	}

	// Print errors
	go func() {
		for err := range group.Errors() {
			fmt.Println("Some error", err)
		}
	}()

	return &Consumer{
		client:  client,
		group:   group,
		topics:  topics,
		handler: handler,
	}, nil
}