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
	sig = make(chan os.Signal)
)

func init() {
	flag.Parse()
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	wg := run(ctx, *kfk, *tpc, *gid)
	log.Infof("quit by signal %s", <-sig)
	cancel()
	wg.Wait()
}

func run(ctx context.Context, servers, topics, groupID string) *sync.WaitGroup {
	brokers := strings.Split(servers, ",")
	cm := make(map[string]*kafka.Reader)
	var wg sync.WaitGroup
	for _, t := range strings.Split(topics, ",") {
		if cm[t] == nil {
			c := kafka.NewReader(kafka.ReaderConfig{
				Brokers:  brokers,
				GroupID:  groupID,
				Topic:    t,
				MinBytes: 1,
				MaxBytes: 10e6,
			})
			cm[t] = c

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

					log.WithFields(log.Fields{
						"topic":     m.Topic,
						"partition": m.Partition,
						"offset":    m.Offset,
					}).Infof("receive: %x", m.Value)
				}
			}(ctx, &wg, c)
		}
	}
	log.Info("start consume...")

	return &wg
}
