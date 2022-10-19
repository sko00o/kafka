package kafkago

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	sk "github.com/sko00o/kafka"
)

type Handler struct {
	ctx     context.Context
	cancel  context.CancelFunc
	reader  *kafka.Reader
	msgChan chan sk.Message
	log     Logger
}

func New(c sk.ConsumerConfig, options ...OptionFunc) (*Handler, error) {
	ctx, cancel := context.WithCancel(context.Background())
	h := &Handler{
		ctx:    ctx,
		cancel: cancel,
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

	cfg := kafka.ReaderConfig{
		Brokers:     c.Addresses,
		GroupTopics: c.Topics,
	}
	if h.log != nil {
		cfg.Logger = kafka.LoggerFunc(h.log.Infof)
		cfg.ErrorLogger = kafka.LoggerFunc(h.log.Errorf)
	}
	if v := c.MinBytes; v > 0 {
		cfg.MinBytes = v
	}
	if v := c.MaxBytes; v > 0 {
		cfg.MaxBytes = v
	}
	if v := c.GroupID; v != "" {
		cfg.GroupID = v
	} else {
		return nil, errors.New("group_id is empty")
	}
	if !c.CommitSync {
		cfg.CommitInterval = 2 * time.Second
		if v := c.CommitInterval; v != 0 {
			cfg.CommitInterval = v
		}
	}
	if v := c.StartOffset; v != "" {
		switch strings.ToLower(v) {
		case "first":
			cfg.StartOffset = kafka.FirstOffset
		case "last":
			cfg.StartOffset = kafka.LastOffset
		}
	}
	if v := c.SessionTimeout; v != 0 {
		cfg.SessionTimeout = v
	}
	if v := c.RebalanceTimeout; v != 0 {
		cfg.RebalanceTimeout = v
	}
	h.reader = kafka.NewReader(cfg)

	return h, nil
}

func (h *Handler) Run() error {
	go func() {
		defer close(h.msgChan)

		for {
			msg, err := h.reader.ReadMessage(h.ctx)
			if err != nil {
				if errors.Is(err, io.EOF) ||
					errors.Is(err, context.Canceled) {
					// reader closed
					return
				}

				if h.log != nil {
					h.log.Errorf("read message: %v", err)
				}
				continue
			}

			h.msgChan <- Message{msg}
		}
	}()

	return nil
}

func (h *Handler) Stop() {
	h.cancel()
	if err := h.reader.Close(); err != nil {
		// will not get error actually
		if h.log != nil {
			h.log.Errorf("stop reader: %v", err)
		}
	}
}

func (h *Handler) Receive() <-chan sk.Message {
	return h.msgChan
}

type Message struct {
	kafka.Message
}

func (m Message) Value() []byte {
	return m.Message.Value
}

func (m Message) Topic() string {
	return m.Message.Topic
}

func (m Message) Partition() int32 {
	return int32(m.Message.Partition)
}

func (m Message) Offset() int64 {
	return m.Message.Offset
}
