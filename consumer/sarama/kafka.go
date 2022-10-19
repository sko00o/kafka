package sarama

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	sk "github.com/sko00o/kafka"
)

type Handler struct {
	ctx     context.Context
	cancel  context.CancelFunc
	group   sarama.ConsumerGroup
	topics  []string
	msgChan chan sk.Message
	log     Logger
}

func New(c sk.ConsumerConfig, options ...OptionFunc) (*Handler, error) {
	ctx, cancel := context.WithCancel(context.Background())
	h := &Handler{
		ctx:    ctx,
		cancel: cancel,
		topics: c.Topics,
	}
	if cnt := int(c.WorkerCnt); cnt > 0 {
		h.msgChan = make(chan sk.Message, cnt)
	} else {
		h.msgChan = make(chan sk.Message, 1)
	}

	// NOTE: we need to set logger in kafka reader, so we call OptionFunc here
	for _, option := range options {
		if err := option(h); err != nil {
			return nil, err
		}
	}

	cfg := sarama.NewConfig()

	if v := c.Version; v != "" {
		version, err := sarama.ParseKafkaVersion(v)
		if err != nil {
			return nil, fmt.Errorf("set kafka version %s: %w", v, err)
		}
		cfg.Version = version
	}

	if c.GroupID == "" {
		return nil, errors.New("group_id is empty")
	}

	if v := c.MinBytes; v > 0 {
		cfg.Consumer.Fetch.Min = int32(v)
	}
	if v := c.MaxBytes; v > 0 {
		cfg.Consumer.Fetch.Max = int32(v)
	}

	if !c.CommitSync {
		cfg.Consumer.Offsets.AutoCommit.Interval = 2 * time.Second
		if v := c.CommitInterval; v != 0 {
			cfg.Consumer.Offsets.AutoCommit.Interval = v
		}
	}
	if v := c.StartOffset; v != "" {
		switch strings.ToLower(v) {
		case "first":
			cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
		case "last":
			cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
		}
	}
	if v := c.SessionTimeout; v != 0 {
		cfg.Consumer.Group.Session.Timeout = v
	}
	if v := c.RebalanceTimeout; v != 0 {
		cfg.Consumer.Group.Rebalance.Timeout = v
	}
	cfg.Consumer.Return.Errors = c.EnableErrors

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validate: %w", err)
	}

	group, err := sarama.NewConsumerGroup(c.Addresses, c.GroupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("new consumer group: %w", err)
	}

	if c.EnableErrors {
		// Track errors
		go func() {
			for err := range group.Errors() {
				if h.log != nil {
					h.log.Errorf("consumer group: %v", err)
				}
			}
		}()
	}
	h.group = group

	return h, nil
}

func (h *Handler) Run() error {
	go func() {
		defer close(h.msgChan)

		for {
			handler := &consumeHandler{
				msgChan: h.msgChan,
			}
			if err := h.group.Consume(h.ctx, h.topics, handler); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) ||
					errors.Is(err, context.Canceled) {
					// reader closed
					return
				}

				if h.log != nil {
					h.log.Errorf("stop reader: %v", err)
				}
				continue
			}
		}
	}()

	return nil
}

func (h *Handler) Stop() {
	h.cancel()
	if err := h.group.Close(); err != nil {
		if h.log != nil {
			h.log.Errorf("stop reader: %v", err)
		}
	}
}

func (h *Handler) Receive() <-chan sk.Message {
	return h.msgChan
}

type consumeHandler struct {
	msgChan chan sk.Message
}

func (consumeHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumeHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h consumeHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.msgChan <- Message{msg}
		sess.MarkMessage(msg, "")
	}
	return nil
}

type Message struct {
	*sarama.ConsumerMessage
}

func (m Message) Value() []byte {
	return m.ConsumerMessage.Value
}

func (m Message) Topic() string {
	return m.ConsumerMessage.Topic
}

func (m Message) Partition() int32 {
	return m.ConsumerMessage.Partition
}

func (m Message) Offset() int64 {
	return m.ConsumerMessage.Offset
}
