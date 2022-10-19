package sarama

import (
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	sk "github.com/sko00o/kafka"
)

type Handler struct {
	Producer
	log Logger
}

// New creates a new kafka producer
func New(c sk.ProducerConfig, options ...OptionFunc) (*Handler, error) {
	h := &Handler{}

	// NOTE: we need to set logger, so we call OptionFunc here
	for _, option := range options {
		if err := option(h); err != nil {
			return nil, err
		}
	}

	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.RequiredAcks(c.RequiredAcks)

	if v := c.Version; v != "" {
		version, err := sarama.ParseKafkaVersion(v)
		if err != nil {
			return nil, fmt.Errorf("set kafka version %s: %w", v, err)
		}
		cfg.Version = version
	}

	if v := c.MaxAttempts; v > 0 {
		cfg.Producer.Retry.Max = v
	}
	if v := c.BufferSize; v > 0 {
		cfg.ChannelBufferSize = c.BufferSize
	}
	if v := c.BatchSize; v > 0 {
		cfg.Producer.Flush.MaxMessages = v
	}
	if v := int(c.BatchBytes); v > 0 {
		cfg.Producer.Flush.Bytes = v
	}
	if v := c.BatchTimeout; v > 0 {
		cfg.Producer.Flush.Frequency = v
	}

	if v := c.Compression; v != "" {
		switch strings.ToLower(v) {
		case "none":
			cfg.Producer.Compression = sarama.CompressionNone
		case "gzip":
			cfg.Producer.Compression = sarama.CompressionGZIP
		case "snappy":
			cfg.Producer.Compression = sarama.CompressionSnappy
		case "lz4":
			cfg.Producer.Compression = sarama.CompressionLZ4
		case "zstd":
			cfg.Producer.Compression = sarama.CompressionZSTD
		default:
			return nil, fmt.Errorf("compression %s not support", v)
		}
	}

	if v := c.DialTimeout; v > 0 {
		cfg.Net.DialTimeout = v
	}
	if v := c.ReadTimeout; v > 0 {
		cfg.Net.ReadTimeout = v
	}
	if v := c.WriteTimeout; v > 0 {
		cfg.Net.WriteTimeout = v
	}

	if v := c.SASL; v != nil {
		cfg.Net.SASL.Enable = true

		switch strings.ToLower(v.Mechanism) {
		case "plain":
			cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "scram", "scram_sha_256":
			cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "scram_sha_512":
			cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		default:
			return nil, fmt.Errorf("sasl machanism %s not support", v.Mechanism)
		}

		cfg.Net.SASL.User = v.Username
		cfg.Net.SASL.Password = v.Password
	}

	if c.Async {
		cfg.Producer.Return.Successes = false
		cfg.Producer.Return.Errors = c.EnableAsyncErrors
	} else {
		cfg.Producer.Return.Successes = true
		cfg.Producer.Return.Errors = true
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validate: %w", err)
	}

	var producer Producer
	if c.Async {
		p, err := sarama.NewAsyncProducer(c.Addresses, cfg)
		if err != nil {
			return nil, fmt.Errorf("new async producer: %w", err)
		}
		producer = &SimpleAsyncProducer{AsyncProducer: p}

		if c.EnableAsyncErrors {
			// Track errors
			go func() {
				for err := range p.Errors() {
					if h.log != nil {
						h.log.Errorf("producer: %s", err.Error())
					}
				}
			}()
		}
	} else {
		p, err := sarama.NewSyncProducer(c.Addresses, cfg)
		if err != nil {
			return nil, fmt.Errorf("new sync producer: %w", err)
		}
		producer = &SimpleSyncProducer{SyncProducer: p}
	}

	h.Producer = producer
	return h, nil
}

func (h *Handler) Stop() {
	if err := h.Producer.Close(); err != nil {
		// will not get error actually
		if h.log != nil {
			h.log.Errorf("stop producer: %v", err)
		}
	}
}

func (h *Handler) Send(topic string, value []byte) error {
	return h.Producer.SendWithKey(topic, nil, value)
}
