package kafkago

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
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

	w := &kafka.Writer{
		Addr:         kafka.TCP(c.Addresses...),
		Async:        c.Async,
		RequiredAcks: kafka.RequiredAcks(c.RequiredAcks),
	}

	if v := c.MaxAttempts; v > 0 {
		w.MaxAttempts = v
	}
	if v := c.BatchSize; v > 0 {
		w.BatchSize = v
	}
	if v := c.BatchBytes; v > 0 {
		w.BatchBytes = v
	}
	if v := c.BatchTimeout; v > 0 {
		w.BatchTimeout = v
	}

	if v := c.Compression; v != "" {
		var cc compress.Compression
		if err := cc.UnmarshalText([]byte(v)); err != nil {
			return nil, fmt.Errorf("unsupport codec %s: %w", v, err)
		}
		w.Compression = cc
	}

	if v := c.ReadTimeout; v > 0 {
		w.ReadTimeout = v
	}
	if v := c.WriteTimeout; v > 0 {
		w.WriteTimeout = v
	}

	if v := c.Balancer; v != "" {
		consistent := c.BalancerConsistent
		switch v {
		case "leastbytes":
			w.Balancer = &kafka.LeastBytes{}
		case "murmur2":
			w.Balancer = &kafka.Murmur2Balancer{Consistent: consistent}
		case "crc32":
			w.Balancer = &kafka.CRC32Balancer{Consistent: consistent}
		default:
			return nil, fmt.Errorf("unsupport balancer %s", v)
		}
	}
	if v := c.SASL; v != nil {
		var mechanism sasl.Mechanism
		var err error
		switch strings.ToLower(v.Mechanism) {
		case "plain":
			mechanism = plain.Mechanism{
				Username: v.Username,
				Password: v.Password,
			}
		case "scram", "scram_sha_256":
			mechanism, err = scram.Mechanism(scram.SHA256, v.Username, v.Password)
			if err != nil {
				return nil, fmt.Errorf("new mechanism scram_sha_256: %w", err)
			}
		case "scram_sha_512":
			mechanism, err = scram.Mechanism(scram.SHA512, v.Username, v.Password)
			if err != nil {
				return nil, fmt.Errorf("new mechanism scram_sha_512: %w", err)
			}
		default:
			return nil, fmt.Errorf("sasl mechanism %s not support", v.Mechanism)
		}

		w.Transport = &kafka.Transport{
			Dial: (&net.Dialer{
				Timeout: 3 * time.Second,
			}).DialContext,
			SASL: mechanism,
		}
	}

	var writer Writer = w
	if v := c.BatchQueueSize; v > 0 {
		if c.Async {
			if h.log != nil {
				h.log.Infof("set async to false, because of batch_queue_size > 0")
			}
			c.Async = false
		}

		writer = &batchWriter{
			Writer: w,
			log:    h.log,
			sema:   make(chan struct{}, v),
			wg:     new(sync.WaitGroup),
		}
	}

	producer := &SimpleKafkaGoProducer{
		Writer:      writer,
		ctx:         context.Background(),
		needCopyMsg: c.Async,
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
