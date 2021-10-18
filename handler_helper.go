package kafka_experiments_v2

import (
	"fmt"
	"github.com/Shopify/sarama"
)

// Handler is a convenience struct for creating a sarama.ConsumerGroupHandler
type Handler struct {
	setup           func(_ sarama.ConsumerGroupSession) error
	cleanup         func(_ sarama.ConsumerGroupSession) error
	messageReceived func(payload []byte) error
}

func (h *Handler) Setup(sess sarama.ConsumerGroupSession) error {
	return h.setup(sess)
}

func (h *Handler) Cleanup(sess sarama.ConsumerGroupSession) error {
	return h.cleanup(sess)
}
func (h *Handler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
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

func NewHandler(messageReceived func(payload []byte) error) *Handler {
	return &Handler{
		setup: func(_ sarama.ConsumerGroupSession) error {
			return nil
		},
		cleanup: func(_ sarama.ConsumerGroupSession) error {
			return nil
		},
		messageReceived: messageReceived,
	}
}
