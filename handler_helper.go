package kafkalib

import (
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
		if err := h.messageReceived(msg.Value); err != nil {
			return err
		}

		sess.MarkMessage(msg, "")
	}
	return nil
}

type HandlerOptions struct {
	Setup   func(sess sarama.ConsumerGroupSession) error
	Cleanup func(sess sarama.ConsumerGroupSession) error
}

func NewHandler(messageReceived func(payload []byte) error, opts ...HandlerOptions) *Handler {
	var (
		defaultSetup   = func(_ sarama.ConsumerGroupSession) error { return nil }
		defaultCleanup = func(_ sarama.ConsumerGroupSession) error { return nil }
	)

	h := &Handler{
		messageReceived: messageReceived,
		setup:           defaultSetup,
		cleanup:         defaultCleanup,
	}

	// Add optional configuration
	if len(opts) > 0 {
		if opts[0].Setup != nil {
			h.setup = opts[0].Setup
		}
		if opts[0].Cleanup != nil {
			h.cleanup = opts[0].Cleanup
		}
	}

	return h
}
