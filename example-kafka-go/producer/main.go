package main

import (
	"bufio"
	"context"
	"flag"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

const (
	kfkServer = "127.0.0.1:9092"
	topic     = "test_topic"
)

var (
	kfk = flag.String("kfk", kfkServer, "set kafka host:port here")
	tpc = flag.String("tpc", topic, "set topic here")
	tmd = flag.Bool("t", false, "type message in console")
	sig = make(chan os.Signal)
)

func init() {
	flag.Parse()
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
}

func main() {
	run(context.Background(), *kfk, *tpc, *tmd)
	log.Infof("quit by signal %s", <-sig)
}

func run(ctx context.Context, servers, topic string, typeMode bool) *sync.WaitGroup {
	brokers := strings.Split(servers, ",")
	p := kafka.NewWriter(kafka.WriterConfig{
		Brokers:   brokers,
		Topic:     topic,
		BatchSize: 1,
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func(ctx context.Context, wg *sync.WaitGroup, p *kafka.Writer) {
		defer func() {
			if err := p.Close(); err != nil {
				log.Fatal(err)
			}
			log.WithField("topic", p.Stats().Topic).Info("closed")
			wg.Done()
		}()

		if typeMode {
			input := bufio.NewScanner(os.Stdin)
			for {
				print("> ")
				input.Scan()
				msg := input.Text()

				start := time.Now()
				if err := p.WriteMessages(ctx, kafka.Message{
					Key:   []byte("input"),
					Value: []byte(msg),
				}); err != nil {
					if err == ctx.Err() {
						return
					}
					log.Error(err)
					continue
				}
				log.Infof("send: %s, spent: %s", string(msg), time.Since(start))
			}
		} else {
			log.Info("send random bytes every 2s...")
			rand.Seed(time.Now().Unix())
			for {
				var msg [4]byte
				rand.Read(msg[:])

				start := time.Now()
				if err := p.WriteMessages(ctx, kafka.Message{
					Key:   []byte("input"),
					Value: msg[:],
				}); err != nil {
					if err == ctx.Err() {
						return
					}
					log.Error(err)
					continue
				}
				log.Infof("send: %x, spent: %s", msg, time.Since(start))

				time.Sleep(2 * time.Second)
			}
		}
	}(ctx, &wg, p)

	return &wg
}
