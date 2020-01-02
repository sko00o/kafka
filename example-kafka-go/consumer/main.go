package main

import (
	"context"
	"flag"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	_ "github.com/segmentio/kafka-go/gzip"
	_ "github.com/segmentio/kafka-go/lz4"
	_ "github.com/segmentio/kafka-go/snappy"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

const (
	kfkServer = "127.0.0.1:9092"
	topics    = "test_topic"
	groupID   = "test_group"
)

var (
	kfk = flag.String("kfk", kfkServer, "set kafka host:port here")
	tpc = flag.String("tpc", topics, "set topics here")
	gid = flag.String("gid", groupID, "set groupID here")
	sc  = flag.Bool("s", false, "enable string convert")
	sig = make(chan os.Signal)
)

func init() {
	flag.Parse()
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	done := run(ctx, *kfk, *tpc, *gid, *sc)
	log.Infof("quit by signal %s", <-sig)
	cancel()
	select {
	case <-sig:
		log.Info("quick quit")
		return
	case <-done:
		log.Info("elegant quit")
		return
	}
}

func run(ctx context.Context, servers, topics, groupID string, stringConvert bool) <-chan struct{} {
	brokers := strings.Split(servers, ",")
	var wg sync.WaitGroup
	for _, t := range strings.Split(topics, ",") {
		topic := strings.TrimSpace(t)
		if topic == "" {
			continue
		}
		c := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			GroupID:     groupID,
			Topic:       topic,
			MinBytes:    1,
			MaxBytes:    10e6,
			StartOffset: kafka.LastOffset,
		})
		s := c.Stats()
		log.Infof("topic: %s", s.Topic)

		wg.Add(1)
		go func(ctx context.Context, wg *sync.WaitGroup, c *kafka.Reader) {
			defer func() {
				if err := c.Close(); err != nil {
					log.Warn(err)
				}
				log.WithField("topic", c.Stats().Topic).Info("closed")
				wg.Done()
			}()

			for {
				m, err := c.ReadMessage(ctx)
				if err != nil {
					if err == io.EOF || err == ctx.Err() {
						break
					}
					log.Error(err)
					continue
				}

				lg := log.WithFields(log.Fields{
					"topic":     m.Topic,
					"partition": m.Partition,
					"offset":    m.Offset,
				})

				if stringConvert {
					lg.Infof("receive: %x (%s)", m.Value, m.Value)
				} else {
					lg.Infof("receive: %x", m.Value)
				}
			}
		}(ctx, &wg, c)
	}
	log.Info("start consume...")
	done := make(chan struct{})
	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	return done
}
