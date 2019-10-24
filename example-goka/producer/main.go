package main

import (
	"bufio"
	"context"
	"flag"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
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
	ctx, cancel := context.WithCancel(context.Background())
	go run(*kfk, *tpc, *tmd)

	for {
		select {
		case s := <-sig:
			log.Infof("quit by signal %s", s)
			cancel()
		case <-ctx.Done():
			return
		}
	}
}

func run(servers, topic string, typeMode bool) {
	brokers := strings.Split(servers, ",")
	l := goka.WithEmitterLogger(log.New())
	p, err := goka.NewEmitter(brokers, goka.Stream(topic), new(codec.Bytes), l)
	if err != nil {
		log.Fatalf("error creating emitter: %v", err)
	}

	go func(p *goka.Emitter) {
		defer func() {
			if err := p.Finish(); err != nil {
				log.Fatal(err)
			}
		}()

		if typeMode {
			input := bufio.NewScanner(os.Stdin)
			for {
				print("> ")
				input.Scan()
				msg := input.Text()

				start := time.Now()
				if err := p.EmitSync("input", []byte(msg)); err != nil {
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
				if err := p.EmitSync("input", msg[:]); err != nil {
					log.Error(err)
					continue
				}
				log.Infof("send: %x, spent: %s", msg, time.Since(start))

				time.Sleep(2 * time.Second)
			}
		}
	}(p)
}
