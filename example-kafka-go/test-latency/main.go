package main

import (
	"context"
	"crypto/rand"
	"flag"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

const (
	kfkServer = "localhost:9092"
	topic     = "test_topic"
	groupID   = "test_group"
)

func main() {
	log.SetFormatter(&log.TextFormatter{TimestampFormat: time.RFC3339Nano})
	kfk := flag.String("kfk", kfkServer, "set kafka host:port here")
	tpc := flag.String("tpc", topic, "set topics here")
	gid := flag.String("gid", groupID, "set groupID here")
	flag.Parse()
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g := &generator{
		brokers:  strings.Split(*kfk, ","),
		groupID:  *gid,
		minBytes: 1,
		maxBytes: 10e6,
		balancer: &kafka.Hash{},
		timeout:  10 * time.Millisecond,
	}

	// consumer
	c := g.newReceiver(*tpc)
	defer func() {
		c.Close()
	}()
	go func(c *kafka.Reader) {
		for {
			m, err := c.ReadMessage(ctx)
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Error(err)
				continue
			}

			log.WithFields(log.Fields{
				"topic":     m.Topic,
				"partition": m.Partition,
				"offset":    m.Offset,
			}).Infof("recv: %s", m.Value)
		}
	}(c)
	log.Info("start consume")

	// producer
	p := g.newSender(*tpc)
	defer func() {
		p.Close()
	}()

	log.Info("start produce")
	for {
		select {
		case <-sig:
			return
		case <-time.After(time.Second * 2):
			var msg [4]byte
			_, _ = rand.Read(msg[:])

			start := time.Now()
			if err := p.WriteMessages(ctx, kafka.Message{
				Key:   []byte("random"),
				Value: msg[:],
			}); err != nil {
				log.Error(err)
				continue
			}
			log.Infof("send: %s, spent: %s", msg, time.Since(start))
		}
	}
}

type generator struct {
	brokers  []string
	groupID  string
	minBytes int
	maxBytes int
	balancer kafka.Balancer
	timeout  time.Duration
}

func (g *generator) newReceiver(topic string) *kafka.Reader {
	log.WithField("topic", topic).Info("setup consume topic")

	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  g.brokers,
		GroupID:  g.groupID,
		Topic:    topic,
		MinBytes: g.minBytes,
		MaxBytes: g.maxBytes,
	})
}

func (g *generator) newSender(topic string) *kafka.Writer {
	log.WithField("topic", topic).Info("setup produce topic")

	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:   g.brokers,
		Topic:     topic,
		BatchSize: 1, // send immediately
	})
}
