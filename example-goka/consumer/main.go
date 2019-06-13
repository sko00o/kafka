package main

import (
	"context"
	"flag"
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
	groupID   = "test_group"
)

var (
	kfk = flag.String("kfk", kfkServer, "set kafka host:port here")
	tpc = flag.String("tpc", topic, "set topics here")
	gid = flag.String("gid", groupID, "set groupID here")
	sc  = flag.Bool("s", false, "enable string convert")
	sig = make(chan os.Signal)
)

func init() {
	flag.Parse()
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	log.SetFormatter(&log.TextFormatter{TimestampFormat: time.RFC3339Nano})
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan interface{})
	run(ctx, done, *kfk, *tpc, *gid, *sc)
	log.Infof("quit by signal %s", <-sig)
	cancel()
	<-done
}

func run(ctx context.Context, done chan interface{}, servers, topics, groupID string, stringConvert bool) {
	h := func(ctx goka.Context, m interface{}) {
		lg := log.WithField("topic", ctx.Topic())

		if stringConvert {
			lg.Infof("receive: %x (%s)", m, m)
		} else {
			lg.Infof("receive: %x", m)
		}
	}

	var es goka.Edges
	for _, t := range strings.Split(topics, ",") {
		e := goka.Input(goka.Stream(t), new(codec.Bytes), h)
		es = append(es, e)
	}
	es = append(es, goka.Persist(new(codec.Int64)))

	g := goka.DefineGroup(goka.Group(groupID), es...)

	l := goka.WithLogger(log.New())

	brokers := strings.Split(servers, ",")
	p, err := goka.NewProcessor(brokers, g, l)
	if err != nil {
		log.Fatalln(err)
	}

	go func() {
		defer close(done)
		if err := p.Run(ctx); err != nil {
			log.Fatal(err)
		}
	}()
	log.Info("start consume...")
}
