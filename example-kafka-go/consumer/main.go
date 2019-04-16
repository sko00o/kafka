package main

import (
	"context"
	"flag"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

const (
	kfkServer = "kafka.domain:9092"
	topic     = "test_topic"
	groupID   = "test_group"
)

func main() {

	kfk := flag.String("kfk", kfkServer, "set kafka host:port here")
	tpc := flag.String("tpc", topic, "set topics here")
	gid := flag.String("gid", groupID, "set groupID here")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())

	g := &generator{
		cBroker: strings.Split(*kfk, ","),
		groupID: *gid,
	}

	cm := make(map[string]*kafka.Reader)

	for _, t := range strings.Split(*tpc, ",") {
		if cm[t] == nil {
			cm[t] = g.newReceiver(t)
		}
	}
	defer func() {
		for _, c := range cm {
			c.Close()
		}
		cancel()
	}()

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)
	stop := make(chan interface{})
	var wg sync.WaitGroup

	for _, c := range cm {
		go func() {
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
				}).Infof("receive: %s", m.Value)
			}
		}()
	}
	log.Info("start consume")

	<-sig
	close(stop)
	wg.Wait()
}

type generator struct {
	cBroker []string
	groupID string
}

func (g *generator) newReceiver(topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  g.cBroker,
		GroupID:  g.groupID,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
}
