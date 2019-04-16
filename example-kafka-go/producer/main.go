package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

const (
	kafkaServers = "kafka.domain:9092"
	topic        = "test_topic"
)

func main() {
	kfk := flag.String("kfk", kafkaServers, "set kafka broker list here")
	tpc := flag.String("tpc", topic, "set topic here")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())

	p := kafka.NewWriter(kafka.WriterConfig{
		Brokers: strings.Split(*kfk, ","),
		Topic:   *tpc,
	})
	defer func() {
		p.Close()
		cancel()
	}()

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)

	log.SetLevel(log.DebugLevel)

	log.Info("start produce")

	for {
		select {
		case <-sig:
			return
		case <-time.After(time.Second * 2):
			msg := time.Now().String()

			if err := p.WriteMessages(ctx, kafka.Message{
				Key:   []byte("time"),
				Value: []byte(msg),
			}); err != nil {
				log.Error(err)
				continue
			}

			log.Infof("send: %s", msg)
		}
	}

}
